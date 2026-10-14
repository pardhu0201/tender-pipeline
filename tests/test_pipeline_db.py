"""Integration tests against a real PostgreSQL.

They DROP AND RECREATE the pipeline's tables, so they only run when
TEST_DATABASE_URL is set explicitly; DATABASE_URL is never used here.
"""

import copy
import csv
import io
import json
import os
import signal
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from tender_pipeline import db, pipeline
from tender_pipeline.cli import main
from tender_pipeline.config import Settings
from tender_pipeline.export import export_tenders
from tender_pipeline.gem_client import GemError, ListingPage
from tender_pipeline.normalize import IST

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL, reason="set TEST_DATABASE_URL to run database tests"
)

SETTINGS = Settings(database_url=TEST_DATABASE_URL or "", incremental_stop_after_pages=3)


class FakeSource:
    """Stands in for GemClient: yields scripted pages, optionally failing on one."""

    def __init__(self, pages, *, total=None, fail_on_page=None, error=None):
        self.pages = pages
        self.total = sum(len(p) for p in pages) if total is None else total
        self.fail_on_page = fail_on_page
        self.error = error or GemError("simulated outage")
        self.requests_made = 0

    @property
    def http_metrics(self):
        return {"requests": self.requests_made}

    def iter_pages(self, sort="", start_page=1, max_pages=None):
        for number, docs in enumerate(self.pages, start=1):
            if number == self.fail_on_page:
                raise self.error
            self.requests_made += 1
            yield ListingPage(number=number, total=self.total, docs=docs)
            if max_pages is not None and number >= max_pages:
                return


def gem_time(moment: datetime) -> str:
    """Format like GeM does: IST wall-clock time with a bogus 'Z'."""
    return moment.astimezone(IST).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"


def synthetic(base: dict, n: int, start: datetime) -> dict:
    """A copy of a real record with its own identity and start time."""
    doc = copy.deepcopy(base)
    doc["id"] = str(50_000_000 + n)
    doc["b_id"] = [50_000_000 + n]
    doc["b_bid_number"] = [f"GEM/2026/B/{90_000_000 + n}"]
    doc["final_start_date_sort"] = [gem_time(start)]
    doc["final_end_date_sort"] = [gem_time(start + timedelta(days=10))]
    return doc


@pytest.fixture
def conn():
    with psycopg.connect(TEST_DATABASE_URL) as connection:
        connection.execute("DROP TABLE IF EXISTS rejected_records, tenders, scrape_runs CASCADE")
        connection.commit()
        db.init_schema(connection)
        yield connection


def count(conn, sql, *params):
    return conn.execute(sql, params).fetchone()[0]


# -- schema --------------------------------------------------------------------


def test_schema_is_idempotent(conn):
    db.init_schema(conn)
    db.init_schema(conn)
    assert count(conn, "SELECT count(*) FROM tenders") == 0


# -- idempotent upserts ----------------------------------------------------------------


def test_rerunning_the_same_data_changes_nothing(conn, listing_docs):
    pages = [listing_docs[:6], listing_docs[6:]]

    first = pipeline.run(conn, FakeSource(pages), SETTINGS, mode="full")
    assert first.status == "success"
    assert (first.stats.inserted, first.stats.updated, first.stats.unchanged) == (12, 0, 0)
    assert first.quality["coverage"] == 1.0

    second = pipeline.run(conn, FakeSource(pages), SETTINGS, mode="full")
    assert (second.stats.inserted, second.stats.updated, second.stats.unchanged) == (0, 0, 12)
    assert count(conn, "SELECT count(*) FROM tenders") == 12
    assert count(conn, "SELECT count(*) FROM tenders WHERE last_run_id = %s", second.run_id) == 12


def test_a_changed_record_is_updated_in_place(conn, listing_docs, fixture_doc):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")

    extended = fixture_doc("GEM/2026/B/8041403")
    extended["final_end_date_sort"] = ["2026-10-20T15:00:00Z"]  # corrigendum: deadline moved
    others = [d for d in listing_docs if d["b_bid_number"] != ["GEM/2026/B/8041403"]]
    result = pipeline.run(conn, FakeSource([others + [extended]]), SETTINGS, mode="full")

    assert (result.stats.inserted, result.stats.updated, result.stats.unchanged) == (0, 1, 11)
    end_at = count(conn, "SELECT end_at FROM tenders WHERE bid_number = 'GEM/2026/B/8041403'")
    assert end_at == datetime(2026, 10, 20, 9, 30, tzinfo=UTC)  # 15:00 IST


def test_ist_times_are_stored_as_the_right_instant(conn, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    end_at = count(conn, "SELECT end_at FROM tenders WHERE bid_number = 'GEM/2026/B/8041403'")
    assert end_at == datetime(2026, 10, 13, 14, 30, tzinfo=UTC)  # "20:00:00Z" = 8 PM IST


def test_full_text_search_finds_tenders(conn, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    hits = conn.execute(
        "SELECT bid_number FROM tenders "
        "WHERE search_vector @@ websearch_to_tsquery('english', 'fire extinguishers')"
    ).fetchall()
    assert hits == [("GEM/2026/B/8114345",)]


# -- validation and quality gates ------------------------------------------------------


def test_invalid_records_are_quarantined_and_the_run_flagged(conn, listing_docs, fixture_doc):
    broken = fixture_doc("GEM/2026/B/8041403")
    broken["b_id"] = [99999999]
    broken["b_bid_number"] = ["BID-1234"]

    result = pipeline.run(conn, FakeSource([listing_docs + [broken]]), SETTINGS, mode="full")

    assert result.stats.rejected == 1 and result.stats.inserted == 12
    assert result.status == "degraded"  # 1 of 13 = 7.7%, above the 5% gate
    reason, source_id, raw = conn.execute(
        "SELECT reason, source_id, raw FROM rejected_records WHERE run_id = %s", (result.run_id,)
    ).fetchone()
    assert (reason, source_id) == ("invalid_bid_number", "99999999")
    assert "b.b_created_by" not in raw


def test_run_bookkeeping_is_recorded(conn, listing_docs):
    result = pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    status, records, quality, finished = conn.execute(
        "SELECT status, records_seen, quality, finished_at FROM scrape_runs WHERE id = %s",
        (result.run_id,),
    ).fetchone()
    assert status == "success" and records == 12 and finished is not None
    assert quality["null_rate"]["ministry"] == 0.25
    assert quality["complete"] is True


# -- incremental crawling ---------------------------------------------------------------


def test_first_incremental_run_falls_back_to_full(conn, listing_docs):
    result = pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="incremental")
    assert result.mode == "full"


def test_incremental_stops_after_consecutive_old_pages(conn, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")

    base, now = listing_docs[2], datetime.now(UTC)
    new_page = [synthetic(base, i, now + timedelta(days=1)) for i in range(10)]
    old_pages = [
        [synthetic(base, 100 * p + i, now - timedelta(days=p + 2)) for i in range(10)]
        for p in range(1, 6)
    ]
    source = FakeSource([new_page, *old_pages], total=60)
    result = pipeline.run(conn, source, SETTINGS, mode="incremental")

    assert result.mode == "incremental"
    assert result.stats.pages_fetched == 4  # 1 new page + 3 old pages in a row, then stop
    assert result.quality["complete"] is True
    assert result.status == "success"


def test_a_capped_run_is_never_the_baseline_for_incremental_runs(conn, listing_docs):
    # A quick `--max-pages` try-out saw only the newest page. If the next
    # incremental run trusted it, everything older would never be fetched.
    pages = [listing_docs[:6], listing_docs[6:]]
    pipeline.run(conn, FakeSource(pages), SETTINGS, mode="full", max_pages=1)

    result = pipeline.run(conn, FakeSource(pages), SETTINGS, mode="incremental")

    assert result.mode == "full"
    assert count(conn, "SELECT count(*) FROM tenders") == 12


def test_max_pages_marks_the_run_incomplete(conn, listing_docs):
    pages = [listing_docs[:4], listing_docs[4:8], listing_docs[8:]]
    result = pipeline.run(conn, FakeSource(pages), SETTINGS, mode="full", max_pages=2)
    assert result.stats.pages_fetched == 2
    assert result.quality["complete"] is False
    assert result.quality["coverage"] is None  # not judged on a partial crawl
    assert result.status == "success"


# -- failure handling ------------------------------------------------------------------


def test_a_failed_run_keeps_earlier_pages_and_records_the_error(conn, listing_docs):
    source = FakeSource([listing_docs[:6], listing_docs[6:]], fail_on_page=2)
    with pytest.raises(GemError):
        pipeline.run(conn, source, SETTINGS, mode="full")

    status, error = conn.execute(
        "SELECT status, error FROM scrape_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert status == "failed" and "simulated outage" in error
    assert count(conn, "SELECT count(*) FROM tenders") == 6  # page 1 was committed

    # A failed run is not a watermark: the next incremental run does a full crawl.
    retry = pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="incremental")
    assert retry.mode == "full" and retry.stats.inserted == 6


def test_runs_whose_process_died_are_marked_abandoned(conn, listing_docs):
    # While we hold the lock, a 'running' row can only belong to a dead
    # process, however recently it started.
    conn.execute("INSERT INTO scrape_runs (source, mode) VALUES ('gem', 'full')")
    conn.commit()
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    assert count(conn, "SELECT count(*) FROM scrape_runs WHERE status = 'abandoned'") == 1


def test_sigterm_mid_crawl_is_recorded(conn, listing_docs):
    stop = pipeline.Terminated("terminated by SIGTERM")
    source = FakeSource([listing_docs[:6], listing_docs[6:]], fail_on_page=2, error=stop)
    with pytest.raises(pipeline.Terminated):
        pipeline.run(conn, source, SETTINGS, mode="full")

    status, error = conn.execute(
        "SELECT status, error FROM scrape_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert (status, error) == ("failed", "terminated by SIGTERM")
    assert count(conn, "SELECT count(*) FROM tenders") == 6


def test_malformed_records_are_quarantined(conn, listing_docs):
    result = pipeline.run(conn, FakeSource([[*listing_docs, "junk"]]), SETTINGS, mode="full")
    assert result.stats.rejected == 1
    reason, raw = conn.execute("SELECT reason, raw FROM rejected_records").fetchone()
    assert (reason, raw) == ("malformed_record", {"value": "'junk'"})


def test_progress_is_logged_on_long_crawls(conn, listing_docs, caplog):
    base, now = listing_docs[2], datetime.now(UTC)
    pages = [[synthetic(base, p, now - timedelta(minutes=p))] for p in range(25)]
    with caplog.at_level("INFO", logger="tender_pipeline.pipeline"):
        pipeline.run(conn, FakeSource(pages, total=1000), SETTINGS, mode="full")
    assert any(m.startswith("page 25/100:") and "min left" in m for m in caplog.messages)


# -- concurrency -------------------------------------------------------------------------


@pytest.fixture
def other_session():
    """A second database session, e.g. a run started from another container."""
    with psycopg.connect(TEST_DATABASE_URL) as connection:
        yield connection


def test_a_second_concurrent_run_is_refused(conn, other_session, listing_docs):
    assert db.try_lock(other_session, "gem")
    with pytest.raises(pipeline.RunInProgress):
        pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    assert count(conn, "SELECT count(*) FROM scrape_runs") == 0  # refused before doing anything

    db.unlock(other_session, "gem")
    other_session.commit()
    assert pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full").status == "success"


def test_the_lock_is_released_after_success_and_after_failure(conn, other_session, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    assert db.try_lock(other_session, "gem")
    db.unlock(other_session, "gem")

    with pytest.raises(GemError):
        pipeline.run(conn, FakeSource([listing_docs], fail_on_page=1), SETTINGS, mode="full")
    assert db.try_lock(other_session, "gem")


def test_reprocess_also_respects_the_lock(conn, other_session):
    assert db.try_lock(other_session, "gem")
    with pytest.raises(pipeline.RunInProgress):
        pipeline.reprocess(conn)


# -- reprocessing ----------------------------------------------------------------------


def test_reprocess_rebuilds_columns_from_raw(conn, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    # Simulate a parser bug that shifted every end time and lost the states.
    conn.execute("UPDATE tenders SET end_at = end_at + interval '330 minutes', state = NULL")
    conn.commit()

    counts = pipeline.reprocess(conn, batch_size=5)  # several keyset pages

    assert counts == {"checked": 12, "changed": 12, "now_invalid": 0}
    state = count(conn, "SELECT state FROM tenders WHERE bid_number = 'GEM/2026/B/8110219'")
    assert state == "Jharkhand"
    end_at = count(conn, "SELECT end_at FROM tenders WHERE bid_number = 'GEM/2026/B/8041403'")
    assert end_at == datetime(2026, 10, 13, 14, 30, tzinfo=UTC)
    assert pipeline.reprocess(conn)["changed"] == 0


def test_reprocess_leaves_rows_that_no_longer_validate_untouched(conn, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    conn.execute(
        "UPDATE tenders SET raw = jsonb_set(raw, '{b_bid_number}', '[\"not-a-bid\"]') "
        "WHERE bid_number = 'GEM/2026/B/8041403'"
    )
    conn.commit()

    assert pipeline.reprocess(conn) == {"checked": 12, "changed": 0, "now_invalid": 1}
    assert count(conn, "SELECT count(*) FROM tenders WHERE bid_number = 'GEM/2026/B/8041403'") == 1


def test_unknown_mode_is_rejected(conn):
    with pytest.raises(ValueError, match="unknown mode"):
        pipeline.run(conn, FakeSource([]), SETTINGS, mode="sometimes")


# -- export ------------------------------------------------------------------------------


def test_export_jsonl_and_csv(conn, listing_docs):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")

    out = io.StringIO()
    assert export_tenders(conn, out, fmt="jsonl") == 12
    records = {r["bid_number"]: r for r in map(json.loads, out.getvalue().splitlines())}
    central = records["GEM/2026/B/8041403"]
    assert central["end_at"] == "2026-10-13T20:00:00+05:30"
    assert central["ministry"] == "Ministry of Education"
    assert "raw" not in central and "content_hash" not in central

    out = io.StringIO()
    assert export_tenders(conn, out, fmt="csv") == 12
    rows = {r["bid_number"]: r for r in csv.DictReader(io.StringIO(out.getvalue()))}
    lamps = rows["GEM/2026/B/8114297"]
    assert lamps["category_ids"] == (
        "home_el05420327_lamp_lamp_se47014418|home_el63626345_lamp_lamp_whit"
    )
    assert lamps["is_high_value"] == "false"


def test_export_open_only(conn, listing_docs):
    base, now = listing_docs[2], datetime.now(UTC)
    closed = synthetic(base, 1, now - timedelta(days=20))  # ended ten days ago
    still_open = synthetic(base, 2, now)  # ends in ten days
    pipeline.run(conn, FakeSource([[closed, still_open]]), SETTINGS, mode="full")

    out = io.StringIO()
    assert export_tenders(conn, out, open_only=True) == 1
    assert json.loads(out.getvalue())["bid_number"] == still_open["b_bid_number"][0]


# -- CLI ---------------------------------------------------------------------------------


@pytest.fixture
def cli_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    previous = signal.getsignal(signal.SIGTERM)
    yield monkeypatch
    signal.signal(signal.SIGTERM, previous)  # `run` installs a SIGTERM handler


def fake_gem(monkeypatch, source):
    monkeypatch.setattr("tender_pipeline.cli.GemClient", lambda settings: source)


def test_cli_end_to_end(conn, listing_docs, cli_env, tmp_path, capsys):
    fake_gem(cli_env, FakeSource([listing_docs]))
    assert main(["init-db"]) == 0
    assert main(["run", "--mode", "full"]) == 0
    assert main(["reprocess"]) == 0

    target = tmp_path / "tenders.csv"
    assert main(["export", "--format", "csv", "--output", str(target)]) == 0
    assert len(target.read_text(encoding="utf-8").splitlines()) == 13  # header + 12 rows

    assert main(["status"]) == 0
    assert "full" in capsys.readouterr().out


def test_cli_skips_quietly_when_another_run_holds_the_lock(
    conn, other_session, listing_docs, cli_env, caplog
):
    fake_gem(cli_env, FakeSource([listing_docs]))
    assert db.try_lock(other_session, "gem")
    assert main(["run"]) == 0
    assert "already in progress" in caplog.text
    assert count(conn, "SELECT count(*) FROM scrape_runs") == 0


def test_cli_exit_codes_for_failure_and_sigterm(conn, listing_docs, cli_env):
    fake_gem(cli_env, FakeSource([listing_docs], fail_on_page=1))
    assert main(["run", "--mode", "full"]) == 1

    stop = pipeline.Terminated("terminated by SIGTERM")
    fake_gem(cli_env, FakeSource([listing_docs], fail_on_page=1, error=stop))
    assert main(["run", "--mode", "full"]) == 143


def test_status_freshness_check(conn, listing_docs, cli_env, capsys):
    assert main(["status", "--max-age-hours", "1"]) == 1  # nothing has run yet
    assert "STALE" in capsys.readouterr().out

    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    assert main(["status", "--max-age-hours", "1"]) == 0
    assert "Fresh" in capsys.readouterr().out


def test_status_explains_degraded_runs(conn, listing_docs, fixture_doc, cli_env, capsys):
    broken = fixture_doc("GEM/2026/B/8041403")
    broken["b_id"], broken["b_bid_number"] = [1], ["BID-1"]
    pipeline.run(conn, FakeSource([[*listing_docs, broken]]), SETTINGS, mode="full")

    assert main(["status"]) == 0
    assert "issue: reject rate 7.7% is above 5%" in capsys.readouterr().out


def test_report_runs(conn, listing_docs, cli_env, capsys):
    pipeline.run(conn, FakeSource([listing_docs]), SETTINGS, mode="full")
    assert main(["report"]) == 0
    assert "Tenders stored: 12" in capsys.readouterr().out
