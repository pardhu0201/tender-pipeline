"""Export clean tenders as JSON Lines or CSV, e.g. as a dataset for analytics or ML.

Rows are streamed through a server-side cursor in batches, so memory use stays
flat however large the table grows; nothing is ever loaded all at once.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from typing import Any, TextIO

import psycopg

from tender_pipeline.normalize import IST

COLUMNS = (
    "bid_number", "bid_type", "parent_bid_number", "items", "items_detail", "quantity",
    "ministry", "department", "state", "start_at", "end_at", "is_high_value",
    "is_single_packet", "is_global_tender", "category_ids", "document_url",
    "source", "source_id", "first_seen_at", "last_seen_at",
)  # fmt: skip
FORMATS = ("jsonl", "csv")
BATCH_ROWS = 2000


def _jsonable(value: Any) -> Any:
    # ISO 8601 with an explicit +05:30 offset: unambiguous, and readable in India.
    return value.astimezone(IST).isoformat() if isinstance(value, datetime) else value


def _csv_cell(value: Any) -> Any:
    value = _jsonable(value)
    if isinstance(value, list):
        return "|".join(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def export_tenders(
    conn: psycopg.Connection, out: TextIO, *, fmt: str = "jsonl", open_only: bool = False
) -> int:
    """Write every tender (or only open ones) to `out`; returns the number of rows."""
    if fmt not in FORMATS:
        raise ValueError(f"format must be one of {FORMATS}")
    where = "WHERE end_at > now()" if open_only else ""
    writer = None
    if fmt == "csv":
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(COLUMNS)

    rows = 0
    with conn.cursor(name="tender_export") as cur:  # a named cursor lives on the server
        cur.itersize = BATCH_ROWS
        cur.execute(f"SELECT {', '.join(COLUMNS)} FROM tenders {where} ORDER BY id")
        for row in cur:
            if writer is None:
                record = {col: _jsonable(val) for col, val in zip(COLUMNS, row, strict=True)}
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
            else:
                writer.writerow([_csv_cell(value) for value in row])
            rows += 1
    return rows
