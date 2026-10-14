"""A plain-text summary of what is in the database (`python -m tender_pipeline report`)."""

from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, TextIO

import psycopg

from tender_pipeline.db import one_row
from tender_pipeline.normalize import IST

OVERVIEW_SQL = """
SELECT count(*)                                                        AS tenders,
       count(*) FILTER (WHERE end_at > now())                          AS open_now,
       count(*) FILTER (WHERE end_at > now()
                          AND end_at <= now() + interval '48 hours')   AS closing_48h,
       count(DISTINCT ministry)                                        AS ministries,
       count(DISTINCT state)                                           AS states,
       max(last_seen_at)                                               AS last_seen
FROM tenders
"""

BY_MINISTRY_SQL = """
SELECT ministry,
       count(*)                                                  AS open_bids,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1)        AS pct
FROM tenders
WHERE end_at > now() AND ministry IS NOT NULL
GROUP BY ministry
ORDER BY open_bids DESC
LIMIT 10
"""

BY_STATE_SQL = """
SELECT state, count(*) AS open_bids
FROM tenders
WHERE end_at > now() AND state IS NOT NULL
GROUP BY state
ORDER BY open_bids DESC
LIMIT 10
"""

CLOSING_SOON_SQL = """
SELECT bid_number,
       left(items, 48)                         AS items,
       coalesce(ministry, state, department)   AS buyer,
       end_at
FROM tenders
WHERE end_at > now()
ORDER BY end_at, bid_number
LIMIT 10
"""


def fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.astimezone(IST).strftime("%d %b %H:%M IST")
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def print_table(out: TextIO, title: str, headers: list[str], rows: list[tuple]) -> None:
    out.write(f"\n{title}\n")
    if not rows:
        out.write("  (nothing yet)\n")
        return
    cells = [[fmt(value) for value in row] for row in rows]
    widths = [max(len(h), *(len(row[i]) for row in cells)) for i, h in enumerate(headers)]
    out.write("  " + "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)) + "\n")
    out.write("  " + "  ".join("-" * w for w in widths) + "\n")
    for row in cells:
        out.write("  " + "  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)) + "\n")


def print_report(conn: psycopg.Connection, out: TextIO | None = None) -> None:
    out = out or sys.stdout  # resolved per call, so redirected stdout is honoured
    tenders, open_now, closing, ministries, states, last_seen = one_row(conn.execute(OVERVIEW_SQL))
    out.write(
        f"Tenders stored: {tenders:,}   open now: {open_now:,}   "
        f"closing within 48h: {closing:,}\n"
        f"Central ministries: {ministries}   states: {states}   "
        f"last refreshed: {fmt(last_seen)}\n"
    )
    print_table(
        out,
        "Open bids by central ministry",
        ["ministry", "open bids", "% of central"],
        conn.execute(BY_MINISTRY_SQL).fetchall(),
    )
    print_table(
        out, "Open bids by state", ["state", "open bids"], conn.execute(BY_STATE_SQL).fetchall()
    )
    print_table(
        out,
        "Closing soonest",
        ["bid number", "items", "buyer", "closes"],
        conn.execute(CLOSING_SOON_SQL).fetchall(),
    )
