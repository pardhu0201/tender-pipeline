"""One pipeline run: crawl -> normalize -> validate -> upsert -> account.

Each page is committed on its own, so a crash loses at most one page and a
re-run simply carries on (the upsert is idempotent). A Postgres advisory lock
guarantees one run per source at a time, wherever runs are started from.

Incremental runs read the listing newest-first and stop once several pages
in a row are older than the last complete run (minus an overlap window).
Offset pagination over a live listing shifts while you read it: newest-first
means new bids push records *down* a page, so a shift shows up as a harmless
duplicate rather than a silently skipped record.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

import psycopg

from tender_pipeline import db
from tender_pipeline.alerts import send_alert
from tender_pipeline.config import Settings
from tender_pipeline.gem_client import PAGE_SIZE, SORT_START_NEWEST, ListingPage
from tender_pipeline.normalize import (
    SOURCE,
    RecordRejected,
    Tender,
    normalize,
    strip_dropped_fields,
    unwrap,
)
from tender_pipeline.quality import QualityTracker

log = logging.getLogger(__name__)

PROGRESS_EVERY_PAGES = 25


class RunInProgress(RuntimeError):
    """Another run of this source holds the pipeline lock."""


class Terminated(KeyboardInterrupt):
    """The process was asked to stop (SIGTERM from `docker stop`, systemd, ...)."""


class ListingSource(Protocol):
    @property
    def http_metrics(self) -> dict[str, Any]: ...

    def iter_pages(
        self, sort: str = ..., start_page: int = 1, max_pages: int | None = None
    ) -> Iterator[ListingPage]: ...


@dataclass(frozen=True)
class RunResult:
    run_id: int
    mode: str
    status: str
    stats: db.RunStats
    quality: dict[str, Any]


def _locked(conn: psycopg.Connection, work: Callable[[], Any]) -> Any:
    """Run `work` holding the source's advisory lock; refuse if someone else has it."""
    if not db.try_lock(conn, SOURCE):
        conn.rollback()
        raise RunInProgress("another run is already in progress - not starting a second one")
    try:
        return work()
    finally:
        try:
            conn.rollback()  # no-op after a clean finish; clears an aborted transaction
            db.unlock(conn, SOURCE)
            conn.commit()
        except psycopg.Error:
            pass  # the connection is gone, and the lock went with it


def run(
    conn: psycopg.Connection,
    source: ListingSource,
    settings: Settings,
    *,
    mode: str = "incremental",
    max_pages: int | None = None,
) -> RunResult:
    if mode not in ("full", "incremental"):
        raise ValueError(f"unknown mode {mode!r}")
    return _locked(conn, lambda: _run(conn, source, settings, mode, max_pages))


def _run(
    conn: psycopg.Connection,
    source: ListingSource,
    settings: Settings,
    mode: str,
    max_pages: int | None,
) -> RunResult:
    abandoned = db.mark_abandoned_runs(conn, SOURCE)
    if abandoned:
        log.warning("marked %d run(s) whose process died as abandoned", abandoned)

    cutoff = None
    if mode == "incremental":
        last = db.last_successful_run(conn, SOURCE, complete_only=True)
        if last is None:
            log.info("no previous complete run to build on - doing a full crawl instead")
            mode = "full"
        else:
            cutoff = last[0] - timedelta(hours=settings.incremental_overlap_hours)
            log.info("incremental: stopping once bids start before %s", cutoff.isoformat())

    run_id = db.start_run(conn, SOURCE, mode)
    conn.commit()
    log.info("run %d started (%s%s)", run_id, mode, f", max {max_pages} pages" if max_pages else "")

    stats = db.RunStats()
    tracker = QualityTracker()
    started = time.monotonic()
    complete = False

    def summarise() -> dict[str, Any]:
        summary = tracker.summary(mode=mode, source_total=stats.source_total, complete=complete)
        summary["complete"] = complete
        summary["http"] = dict(source.http_metrics)
        summary["duration_seconds"] = round(time.monotonic() - started, 1)
        return summary

    try:
        old_pages_in_a_row = 0
        for page in source.iter_pages(sort=SORT_START_NEWEST, max_pages=max_pages):
            accepted = _process_page(conn, page, run_id, stats, tracker)
            db.update_run_progress(conn, run_id, stats)
            conn.commit()
            _log_progress(stats, started, mode)

            if cutoff is not None:
                newest = max((t.start_at for t in accepted), default=None)
                if newest is not None:
                    old_pages_in_a_row = old_pages_in_a_row + 1 if newest < cutoff else 0
                if old_pages_in_a_row >= settings.incremental_stop_after_pages:
                    log.info("%d pages in a row older than the cutoff - done", old_pages_in_a_row)
                    complete = True
                    break
        else:
            # The listing ran out, unless we stopped because of --max-pages.
            complete = max_pages is None or stats.pages_fetched < max_pages

        quality = summarise()
        status = "degraded" if quality["issues"] else "success"
        db.finish_run(conn, run_id, status, stats, quality)
        conn.commit()
    except KeyboardInterrupt as exc:  # Ctrl+C, or Terminated on SIGTERM
        _record_failure(conn, run_id, stats, summarise(), str(exc) or "interrupted by user")
        raise
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _record_failure(conn, run_id, stats, summarise(), error)
        send_alert(settings, f"tender-pipeline run {run_id} FAILED", error)
        raise

    if status == "degraded":
        log.warning("run %d degraded: %s", run_id, "; ".join(quality["issues"]))
        send_alert(settings, f"tender-pipeline run {run_id} degraded", "\n".join(quality["issues"]))
    log.info(
        "run %d %s in %.0fs: %d pages, %d records (%d new, %d changed, %d unchanged, %d rejected)",
        run_id,
        status,
        quality["duration_seconds"],
        stats.pages_fetched,
        stats.records_seen,
        stats.inserted,
        stats.updated,
        stats.unchanged,
        stats.rejected,
    )
    return RunResult(run_id=run_id, mode=mode, status=status, stats=stats, quality=quality)


def _process_page(
    conn: psycopg.Connection,
    page: ListingPage,
    run_id: int,
    stats: db.RunStats,
    tracker: QualityTracker,
) -> list[Tender]:
    stats.pages_fetched += 1
    if stats.source_total is None:
        stats.source_total = page.total

    accepted: list[Tender] = []
    for doc in page.docs:
        stats.records_seen += 1
        try:
            tender = normalize(doc)
        except RecordRejected as exc:
            raw = strip_dropped_fields(doc) if isinstance(doc, dict) else {"value": repr(doc)}
            source_id = unwrap(raw, "b_id")
            source_id = None if source_id is None else str(source_id)
            db.record_rejection(conn, run_id, SOURCE, source_id, exc.reason, exc.detail, raw)
            tracker.observe_rejection(exc.reason, source_id)
            stats.rejected += 1
            continue
        tracker.observe(tender)
        accepted.append(tender)

    counts = db.upsert_tenders(conn, accepted, run_id)
    stats.inserted += counts.inserted
    stats.updated += counts.updated
    stats.unchanged += counts.unchanged
    return accepted


def _record_failure(
    conn: psycopg.Connection, run_id: int, stats: db.RunStats, quality: dict, error: str
) -> None:
    log.error("run %d failed: %s", run_id, error)
    try:
        conn.rollback()  # drop the half-written page; earlier pages are already committed
        db.finish_run(conn, run_id, "failed", stats, quality, error=error)
        conn.commit()
    except psycopg.Error as exc:  # e.g. the database itself went away
        log.error("could not record the failure of run %d: %s", run_id, exc)


def _log_progress(stats: db.RunStats, started: float, mode: str) -> None:
    if stats.pages_fetched % PROGRESS_EVERY_PAGES:
        return
    elapsed = time.monotonic() - started
    rate = stats.pages_fetched / elapsed if elapsed else 0.0
    total_pages = math.ceil(stats.source_total / PAGE_SIZE) if stats.source_total else 0
    eta = ""
    if mode == "full" and rate and total_pages > stats.pages_fetched:
        eta = f", ~{(total_pages - stats.pages_fetched) / rate / 60:.0f} min left"
    log.info(
        "page %d/%d: %d records (%d new, %d changed, %d rejected), %.2f pages/s%s",
        stats.pages_fetched,
        total_pages,
        stats.records_seen,
        stats.inserted,
        stats.updated,
        stats.rejected,
        rate,
        eta,
    )


def reprocess(conn: psycopg.Connection, batch_size: int = 1000) -> dict[str, int]:
    """Re-derive every row's columns from its stored raw payload (after a parser fix)."""
    return _locked(conn, lambda: _reprocess(conn, batch_size))


def _reprocess(conn: psycopg.Connection, batch_size: int) -> dict[str, int]:
    counts = {"checked": 0, "changed": 0, "now_invalid": 0}
    after_id = 0
    while batch := db.fetch_stored_batch(conn, after_id, batch_size):
        updates = []
        for row_id, raw, current in batch:
            counts["checked"] += 1
            try:
                fresh = db.derived_values(normalize(raw))
            except RecordRejected as exc:
                counts["now_invalid"] += 1
                log.warning("row %d no longer passes validation (%s); left as is", row_id, exc)
                continue
            if fresh != current:
                updates.append((row_id, fresh))
        db.update_derived(conn, updates)
        conn.commit()
        counts["changed"] += len(updates)
        after_id = batch[-1][0]
    return counts
