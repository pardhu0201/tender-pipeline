"""PostgreSQL access: schema setup, idempotent upserts and run bookkeeping."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from tender_pipeline.normalize import Tender

# Columns derived from `raw` by normalize(). `reprocess` rewrites exactly these.
DERIVED_COLUMNS = (
    "bid_number", "bid_type", "parent_bid_number", "items", "items_detail", "quantity",
    "ministry", "department", "state", "start_at", "end_at", "is_high_value",
    "is_single_packet", "is_global_tender", "category_ids", "document_url",
)  # fmt: skip

UPSERT_TENDER_SQL = """
INSERT INTO tenders (
    source, source_id, bid_number, bid_type, parent_bid_number,
    items, items_detail, quantity, ministry, department, state,
    start_at, end_at, is_high_value, is_single_packet, is_global_tender,
    category_ids, document_url, raw, content_hash, last_run_id
) VALUES (
    %(source)s, %(source_id)s, %(bid_number)s, %(bid_type)s, %(parent_bid_number)s,
    %(items)s, %(items_detail)s, %(quantity)s, %(ministry)s, %(department)s, %(state)s,
    %(start_at)s, %(end_at)s, %(is_high_value)s, %(is_single_packet)s, %(is_global_tender)s,
    %(category_ids)s, %(document_url)s, %(raw)s, %(content_hash)s, %(run_id)s
)
ON CONFLICT (source, source_id) DO UPDATE SET
    bid_number        = EXCLUDED.bid_number,
    bid_type          = EXCLUDED.bid_type,
    parent_bid_number = EXCLUDED.parent_bid_number,
    items             = EXCLUDED.items,
    items_detail      = EXCLUDED.items_detail,
    quantity          = EXCLUDED.quantity,
    ministry          = EXCLUDED.ministry,
    department        = EXCLUDED.department,
    state             = EXCLUDED.state,
    start_at          = EXCLUDED.start_at,
    end_at            = EXCLUDED.end_at,
    is_high_value     = EXCLUDED.is_high_value,
    is_single_packet  = EXCLUDED.is_single_packet,
    is_global_tender  = EXCLUDED.is_global_tender,
    category_ids      = EXCLUDED.category_ids,
    document_url      = EXCLUDED.document_url,
    raw               = EXCLUDED.raw,
    content_hash      = EXCLUDED.content_hash,
    last_seen_at      = now(),
    updated_at        = now(),
    last_run_id       = EXCLUDED.last_run_id
"""


@dataclass
class RunStats:
    pages_fetched: int = 0
    records_seen: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0
    source_total: int | None = None


@dataclass(frozen=True)
class UpsertCounts:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, autocommit=False)


def one_row(cur: psycopg.Cursor) -> tuple[Any, ...]:
    """The row a statement is guaranteed to return (aggregates, INSERT ... RETURNING)."""
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("expected a row from the database, got none")
    return row


def init_schema(conn: psycopg.Connection) -> None:
    sql = resources.files("tender_pipeline").joinpath("sql/schema.sql").read_text("utf-8")
    conn.execute(sql)  # no parameters, so psycopg runs the whole script
    conn.commit()


def derived_values(tender: Tender) -> dict[str, Any]:
    values = {column: getattr(tender, column) for column in DERIVED_COLUMNS}
    values["category_ids"] = list(tender.category_ids)
    return values


def upsert_tenders(conn: psycopg.Connection, tenders: list[Tender], run_id: int) -> UpsertCounts:
    """Insert new tenders, rewrite changed ones, and only touch `last_seen_at` on the rest.

    Change detection compares content hashes first. An unchanged record never
    has its payload re-sent or any indexed column modified, so Postgres can
    apply the update as a cheap HOT update; `updated_at` keeps meaning "when
    this record really changed"; and the run can report exactly how many
    records were new, changed or unchanged.
    """
    batch = {tender.source_id: tender for tender in tenders}  # last copy wins within a page
    if not batch:
        return UpsertCounts()
    sources = {tender.source for tender in batch.values()}
    if len(sources) != 1:
        raise ValueError(f"one source per batch, got {sorted(sources)}")
    source = sources.pop()

    existing: dict[str, str] = dict(
        conn.execute(
            "SELECT source_id, content_hash FROM tenders WHERE source = %s AND source_id = ANY(%s)",
            (source, list(batch)),
        ).fetchall()
    )
    changed = [t for sid, t in batch.items() if existing.get(sid) != t.content_hash]
    unchanged_ids = [sid for sid, t in batch.items() if existing.get(sid) == t.content_hash]

    with conn.cursor() as cur:
        if changed:
            cur.executemany(
                UPSERT_TENDER_SQL,
                [
                    derived_values(t)
                    | {
                        "source": t.source,
                        "source_id": t.source_id,
                        "raw": Jsonb(t.raw),
                        "content_hash": t.content_hash,
                        "run_id": run_id,
                    }
                    for t in changed
                ],
            )
        if unchanged_ids:
            cur.execute(
                "UPDATE tenders SET last_seen_at = now(), last_run_id = %s "
                "WHERE source = %s AND source_id = ANY(%s)",
                (run_id, source, unchanged_ids),
            )

    inserted = sum(1 for t in changed if t.source_id not in existing)
    return UpsertCounts(
        inserted=inserted, updated=len(changed) - inserted, unchanged=len(unchanged_ids)
    )


def record_rejection(
    conn: psycopg.Connection,
    run_id: int,
    source: str,
    source_id: str | None,
    reason: str,
    detail: str,
    raw: dict[str, Any],
) -> None:
    conn.execute(
        "INSERT INTO rejected_records (run_id, source, source_id, reason, detail, raw) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (run_id, source, source_id, reason, detail or None, Jsonb(raw)),
    )


# -- run bookkeeping -------------------------------------------------------


def start_run(conn: psycopg.Connection, source: str, mode: str) -> int:
    return one_row(
        conn.execute(
            "INSERT INTO scrape_runs (source, mode) VALUES (%s, %s) RETURNING id", (source, mode)
        )
    )[0]


def update_run_progress(conn: psycopg.Connection, run_id: int, stats: RunStats) -> None:
    conn.execute(
        """
        UPDATE scrape_runs
        SET pages_fetched = %s, records_seen = %s, inserted = %s, updated = %s,
            unchanged = %s, rejected = %s, source_total = %s
        WHERE id = %s
        """,
        (
            stats.pages_fetched,
            stats.records_seen,
            stats.inserted,
            stats.updated,
            stats.unchanged,
            stats.rejected,
            stats.source_total,
            run_id,
        ),
    )


def finish_run(
    conn: psycopg.Connection,
    run_id: int,
    status: str,
    stats: RunStats,
    quality: dict[str, Any],
    error: str | None = None,
) -> None:
    update_run_progress(conn, run_id, stats)
    conn.execute(
        "UPDATE scrape_runs SET status = %s, finished_at = now(), quality = %s, error = %s "
        "WHERE id = %s",
        (status, Jsonb(quality), error, run_id),
    )


def try_lock(conn: psycopg.Connection, source: str) -> bool:
    """Take this source's pipeline lock; False if another session holds it.

    A session-level advisory lock: it survives commits and rollbacks and is
    released automatically when the connection closes, even if the process is
    killed. So while we hold it, no other run of this source is alive anywhere,
    whichever machine or container started it.
    """
    row = one_row(
        conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (f"tender_pipeline:{source}",)
        )
    )
    return bool(row[0])


def unlock(conn: psycopg.Connection, source: str) -> None:
    conn.execute(
        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (f"tender_pipeline:{source}",)
    )


def mark_abandoned_runs(conn: psycopg.Connection, source: str) -> int:
    """Close out runs whose process died without recording an outcome.

    Only call this while holding the source's lock (`try_lock`): then any run
    still marked 'running' cannot be alive, so no age heuristic is needed.
    """
    cur = conn.execute(
        """
        UPDATE scrape_runs
        SET status = 'abandoned', finished_at = now(),
            error = coalesce(error, 'process exited without finishing the run')
        WHERE source = %s AND status = 'running'
        """,
        (source,),
    )
    return cur.rowcount


def last_successful_run(
    conn: psycopg.Connection, source: str, *, complete_only: bool = False
) -> tuple[datetime, datetime] | None:
    """(started_at, finished_at) of the latest successful run.

    With `complete_only`, only runs that read their whole range count: a run
    cut short by --max-pages must never become an incremental baseline.
    """
    row = conn.execute(
        """
        SELECT started_at, finished_at FROM scrape_runs
        WHERE source = %s AND status IN ('success', 'degraded')
          AND (NOT %s OR (quality ->> 'complete')::boolean)
        ORDER BY started_at DESC LIMIT 1
        """,
        (source, complete_only),
    ).fetchone()
    return (row[0], row[1]) if row else None


def recent_runs(conn: psycopg.Connection, source: str, limit: int = 10) -> list[tuple]:
    return conn.execute(
        """
        SELECT id, mode, status, started_at, finished_at - started_at AS duration,
               pages_fetched, records_seen, inserted, updated, rejected,
               (quality ->> 'coverage')::float AS coverage, error,
               coalesce(quality -> 'issues', '[]'::jsonb) AS issues
        FROM scrape_runs WHERE source = %s
        ORDER BY started_at DESC LIMIT %s
        """,
        (source, limit),
    ).fetchall()


# -- reprocessing ----------------------------------------------------------


def fetch_stored_batch(
    conn: psycopg.Connection, after_id: int, limit: int
) -> list[tuple[int, dict[str, Any], dict[str, Any]]]:
    """Keyset-paginated (id, raw, current derived values); never OFFSET over a big table."""
    rows = conn.execute(
        f"SELECT id, raw, {', '.join(DERIVED_COLUMNS)} FROM tenders "
        "WHERE id > %s ORDER BY id LIMIT %s",
        (after_id, limit),
    ).fetchall()
    return [(row[0], row[1], dict(zip(DERIVED_COLUMNS, row[2:], strict=True))) for row in rows]


def update_derived(conn: psycopg.Connection, updates: list[tuple[int, dict[str, Any]]]) -> None:
    if not updates:
        return
    assignments = ", ".join(f"{column} = %({column})s" for column in DERIVED_COLUMNS)
    with conn.cursor() as cur:
        cur.executemany(
            f"UPDATE tenders SET {assignments}, updated_at = now() WHERE id = %(id)s",
            [values | {"id": row_id} for row_id, values in updates],
        )
