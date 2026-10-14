"""Keeps `tender_embeddings` in step with `tenders`.

Incremental like the crawler: a tender is embedded when it is new, when its
source record changed (content_hash differs), or when the model changed.
Batches are keyset-paginated and committed one at a time, so an interrupted
run loses at most one batch and the next run carries on.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from importlib import resources

import psycopg
from pgvector.psycopg import register_vector

from tender_search.embedder import Embedder

log = logging.getLogger(__name__)

PENDING_SQL = """
SELECT t.id, t.content_hash, coalesce(t.items_detail, t.items, ''),
       t.ministry, t.department, t.state
FROM tenders t
LEFT JOIN tender_embeddings e ON e.tender_id = t.id
WHERE t.id > %(after)s
  AND (%(rebuild)s OR e.tender_id IS NULL
       OR e.content_hash <> t.content_hash OR e.model <> %(model)s)
ORDER BY t.id
LIMIT %(limit)s
"""

UPSERT_SQL = """
INSERT INTO tender_embeddings (tender_id, model, content_hash, embedding)
VALUES (%s, %s, %s, %s)
ON CONFLICT (tender_id) DO UPDATE SET
    model = EXCLUDED.model,
    content_hash = EXCLUDED.content_hash,
    embedding = EXCLUDED.embedding,
    embedded_at = now()
"""


@dataclass(frozen=True)
class IndexStats:
    embedded: int
    seconds: float


def init_schema(conn: psycopg.Connection) -> None:
    sql = resources.files("tender_search").joinpath("sql/schema.sql").read_text("utf-8")
    conn.execute(sql)
    conn.commit()
    register_vector(conn)


def document_text(
    items: str, ministry: str | None, department: str | None, state: str | None
) -> str:
    """What gets embedded: what is being bought, then who is buying it.

    GeM item lists often repeat the same item over and over ("Taxi Hiring -
    Sedan; Local, Taxi Hiring - Sedan; Local, ..."): 27% of all item text in a
    sample of 17,533 tenders. Repeats add nothing to the meaning but would
    weigh on the embedding, so exact repeated segments are dropped.
    """
    seen: dict[str, str] = {}
    for segment in items.split(","):
        segment = segment.strip()
        if segment and segment.lower() not in seen:
            seen[segment.lower()] = segment
    what = ", ".join(seen.values())
    buyer = ", ".join(part for part in (ministry, department, state) if part)
    return f"{what}. Buyer: {buyer}" if buyer else what


def embed_pending(
    conn: psycopg.Connection, embedder: Embedder, batch_size: int = 256, rebuild: bool = False
) -> IndexStats:
    """Embed every tender that needs it; with `rebuild`, re-embed everything.

    A rebuild overwrites rows in place, so search keeps working throughout.
    """
    register_vector(conn)
    started = time.monotonic()
    embedded, after, batches = 0, 0, 0
    while True:
        params = {"after": after, "model": embedder.name, "limit": batch_size, "rebuild": rebuild}
        rows = conn.execute(PENDING_SQL, params).fetchall()
        if not rows:
            break
        texts = [
            document_text(items, ministry, dept, state)
            for _, _, items, ministry, dept, state in rows
        ]
        vectors = embedder.embed_documents(texts)
        with conn.cursor() as cur:
            cur.executemany(
                UPSERT_SQL,
                [
                    (row[0], embedder.name, row[1], vector)
                    for row, vector in zip(rows, vectors, strict=True)
                ],
            )
        conn.commit()
        embedded += len(rows)
        after = rows[-1][0]
        batches += 1
        if batches % 20 == 0:
            rate = embedded / max(time.monotonic() - started, 1e-9)
            log.info("embedded %d tenders (%.0f/s)", embedded, rate)

    seconds = time.monotonic() - started
    log.info("embedded %d tenders in %.1fs with %s", embedded, seconds, embedder.name)
    return IndexStats(embedded=embedded, seconds=round(seconds, 1))
