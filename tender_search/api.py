"""HTTP API (and a small search page) over the hybrid search."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import resources

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pgvector.psycopg import register_vector
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from tender_pipeline.normalize import IST
from tender_search import search as engine
from tender_search.embedder import Embedder


class TenderHit(BaseModel):
    bid_number: str
    bid_type: str
    items: str | None
    buyer: str | None
    ministry: str | None
    department: str | None
    state: str | None
    quantity: int | None
    starts: str  # ISO 8601 in IST
    closes: str
    document_url: str
    score: float
    matched_by: list[str]


class SearchResponse(BaseModel):
    query: str
    count: int
    took_ms: float
    results: list[TenderHit]


def _to_model(hit: engine.Hit) -> TenderHit:
    return TenderHit(
        bid_number=hit.bid_number,
        bid_type=hit.bid_type,
        items=hit.items,
        buyer=hit.buyer,
        ministry=hit.ministry,
        department=hit.department,
        state=hit.state,
        quantity=hit.quantity,
        starts=hit.start_at.astimezone(IST).isoformat(),
        closes=hit.end_at.astimezone(IST).isoformat(),
        document_url=hit.document_url,
        score=hit.score,
        matched_by=list(hit.matched_by),
    )


def create_app(database_url: str, embedder: Embedder, *, pool_size: int = 4) -> FastAPI:
    pool = ConnectionPool(
        database_url,
        min_size=1,
        max_size=pool_size,
        open=False,
        configure=register_vector,
        kwargs={"autocommit": False},
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool.open(wait=True, timeout=30)
        embedder.embed_query("warm up")  # load the model now, not on the first request
        yield
        pool.close()

    app = FastAPI(
        title="Tender Search",
        version="0.1.0",
        summary="Hybrid semantic + keyword search over Indian government tenders (GeM).",
        lifespan=lifespan,
    )

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def page() -> str:
        return resources.files("tender_search").joinpath("static/index.html").read_text("utf-8")

    @app.get("/health")
    def health() -> dict[str, object]:
        with pool.connection() as conn:
            tenders, embedded = conn.execute(
                "SELECT (SELECT count(*) FROM tenders), (SELECT count(*) FROM tender_embeddings)"
            ).fetchone() or (0, 0)
        return {"status": "ok", "tenders": tenders, "embedded": embedded, "model": embedder.name}

    @app.get("/search", response_model=SearchResponse)
    def search(
        q: str = Query(..., min_length=1, max_length=200, description="What you are looking for"),
        state: str | None = Query(None, description="Exact state, e.g. Odisha"),
        ministry: str | None = Query(None, description="Part of a ministry name, e.g. defence"),
        open_only: bool = Query(True, description="Only bids that are still open"),
        closing_within_hours: float | None = Query(None, gt=0, le=24 * 365),
        limit: int = Query(20, ge=1, le=50),
    ) -> SearchResponse:
        filters = engine.Filters(state, ministry, open_only, closing_within_hours)
        started = time.perf_counter()
        with pool.connection() as conn:
            hits = engine.search(conn, embedder, q, filters, limit)
        took_ms = round((time.perf_counter() - started) * 1000, 1)
        return SearchResponse(
            query=q, count=len(hits), took_ms=took_ms, results=[_to_model(h) for h in hits]
        )

    @app.get("/similar", response_model=list[TenderHit])
    def similar(
        bid: str = Query(..., description="A bid number, e.g. GEM/2026/B/8114345"),
        open_only: bool = Query(True),
        limit: int = Query(10, ge=1, le=50),
    ) -> list[TenderHit]:
        with pool.connection() as conn:
            hits = engine.similar(conn, bid, engine.Filters(open_only=open_only), limit)
        if hits is None:
            raise HTTPException(404, f"no embedded tender with bid number {bid!r}")
        return [_to_model(h) for h in hits]

    @app.get("/facets")
    def facets() -> dict[str, list[dict[str, object]]]:
        with pool.connection() as conn:
            result = engine.facets(conn)
        return {
            name: [{"value": value, "open_bids": count} for value, count in rows]
            for name, rows in result.items()
        }

    return app
