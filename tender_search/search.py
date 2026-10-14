"""Hybrid search: semantic (pgvector) and keyword (Postgres full-text) results,
merged with Reciprocal Rank Fusion.

Why hybrid: embeddings find "fire safety equipment" when the tender says
"Portable Fire Extinguishers", but they blur exact identifiers such as
"IS 15683" or a part number, which keyword search matches precisely. RRF
combines the two by rank rather than raw score, so nothing needs calibrating:
a document ranked well by either retriever rises, and one ranked well by both
rises furthest.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg

from tender_search.embedder import Embedder

RRF_K = 60  # the constant from the original RRF paper; damps the very top ranks
CANDIDATES = 50  # how deep each retriever looks before fusion

SEMANTIC_SQL = """
WITH nearest AS MATERIALIZED (
    SELECT t.id, e.embedding <=> %(qvec)s AS distance
    FROM tender_embeddings e
    JOIN tenders t ON t.id = e.tender_id
    WHERE {where}
    ORDER BY distance
    LIMIT %(k)s
)
SELECT id, distance FROM nearest ORDER BY distance, id
"""

KEYWORD_SQL = """
SELECT t.id
FROM tenders t, websearch_to_tsquery('english', %(q)s) AS query
WHERE t.search_vector @@ query AND {where}
ORDER BY ts_rank_cd(t.search_vector, query) DESC, t.id
LIMIT %(k)s
"""

DETAILS_SQL = """
SELECT id, bid_number, bid_type, items, coalesce(ministry, state, department) AS buyer,
       ministry, department, state, quantity, start_at, end_at, document_url
FROM tenders WHERE id = ANY(%s)
"""
_DETAIL_FIELDS = (
    "bid_number", "bid_type", "items", "buyer", "ministry", "department", "state",
    "quantity", "start_at", "end_at", "document_url",
)  # fmt: skip


@dataclass(frozen=True)
class Filters:
    state: str | None = None
    ministry: str | None = None  # substring, case-insensitive: "defence" works
    open_only: bool = True
    closing_within_hours: float | None = None


@dataclass(frozen=True)
class Hit:
    bid_number: str
    bid_type: str
    items: str | None
    buyer: str | None
    ministry: str | None
    department: str | None
    state: str | None
    quantity: int | None
    start_at: datetime
    end_at: datetime
    document_url: str
    score: float
    matched_by: tuple[str, ...]


def reciprocal_rank_fusion(
    rankings: Mapping[str, Sequence[int]], k: int = RRF_K
) -> list[tuple[int, float, tuple[str, ...]]]:
    """Merge ranked id lists: score = sum of 1 / (k + rank) over the lists an id appears in.

    Returns (id, score, names of the lists it came from), best first; ties
    break on id so the order is deterministic.
    """
    scores: dict[int, float] = defaultdict(float)
    sources: dict[int, list[str]] = defaultdict(list)
    for name, ranking in rankings.items():
        for rank, item in enumerate(ranking, start=1):
            scores[item] += 1.0 / (k + rank)
            sources[item].append(name)
    ordered = sorted(scores, key=lambda item: (-scores[item], item))
    return [(item, scores[item], tuple(sources[item])) for item in ordered]


def _like_pattern(text: str) -> str:
    escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def filter_clause(filters: Filters) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = ["TRUE"]
    params: dict[str, Any] = {}
    if filters.state:
        clauses.append("lower(t.state) = lower(%(state)s)")
        params["state"] = filters.state.strip()
    if filters.ministry:
        clauses.append("t.ministry ILIKE %(ministry)s")
        params["ministry"] = _like_pattern(filters.ministry.strip())
    if filters.open_only:
        clauses.append("t.end_at > now()")
    if filters.closing_within_hours is not None:
        clauses.append("t.end_at <= now() + %(closing_hours)s * interval '1 hour'")
        params["closing_hours"] = filters.closing_within_hours
    return " AND ".join(clauses), params


def _relaxed_hnsw(conn: psycopg.Connection) -> None:
    # pgvector 0.8 iterative scan: with filters, keep walking the HNSW graph
    # until LIMIT rows pass them, instead of filtering a fixed candidate set
    # down to too few. "relaxed" order is re-sorted exactly in SEMANTIC_SQL.
    conn.execute("SET LOCAL hnsw.iterative_scan = relaxed_order")


def search(
    conn: psycopg.Connection,
    embedder: Embedder,
    query: str,
    filters: Filters | None = None,
    limit: int = 20,
) -> list[Hit]:
    query = query.strip()
    if not query:
        return []
    where, params = filter_clause(filters or Filters())
    _relaxed_hnsw(conn)
    semantic = conn.execute(
        SEMANTIC_SQL.format(where=where),
        params | {"qvec": embedder.embed_query(query), "k": CANDIDATES},
    ).fetchall()
    keyword = conn.execute(
        KEYWORD_SQL.format(where=where), params | {"q": query, "k": CANDIDATES}
    ).fetchall()
    fused = reciprocal_rank_fusion(
        {"semantic": [row[0] for row in semantic], "keyword": [row[0] for row in keyword]}
    )
    return _hydrate(conn, fused[:limit])


def similar(
    conn: psycopg.Connection, bid_number: str, filters: Filters | None = None, limit: int = 10
) -> list[Hit] | None:
    """Tenders closest in meaning to this one; None if it is unknown or not yet embedded."""
    row = conn.execute(
        "SELECT t.id, e.embedding FROM tenders t "
        "JOIN tender_embeddings e ON e.tender_id = t.id WHERE t.bid_number = %s",
        (bid_number,),
    ).fetchone()
    if row is None:
        return None
    source_id, vector = row
    where, params = filter_clause(filters or Filters())
    _relaxed_hnsw(conn)
    nearest = conn.execute(
        SEMANTIC_SQL.format(where=f"{where} AND t.id <> %(source_id)s"),
        params | {"qvec": vector, "k": limit, "source_id": source_id},
    ).fetchall()
    ranked = [
        (tender_id, round(1.0 - float(distance), 4), ("semantic",))
        for tender_id, distance in nearest
    ]
    return _hydrate(conn, ranked)


def _hydrate(
    conn: psycopg.Connection, ranked: Sequence[tuple[int, float, tuple[str, ...]]]
) -> list[Hit]:
    if not ranked:
        return []
    rows = {row[0]: row for row in conn.execute(DETAILS_SQL, ([r[0] for r in ranked],))}
    hits = []
    for tender_id, score, sources in ranked:
        row = rows.get(tender_id)
        if row is None:  # deleted between the two queries
            continue
        fields = dict(zip(_DETAIL_FIELDS, row[1:], strict=True))
        hits.append(Hit(**fields, score=round(score, 6), matched_by=sources))
    return hits


def facets(conn: psycopg.Connection) -> dict[str, list[tuple[str, int]]]:
    """Open-bid counts per state and per ministry, for filter dropdowns."""
    result = {}
    for column in ("state", "ministry"):
        result[column] = conn.execute(
            f"SELECT {column}, count(*) FROM tenders "
            f"WHERE end_at > now() AND {column} IS NOT NULL GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return result
