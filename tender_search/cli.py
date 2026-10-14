"""Command-line entry point: `python -m tender_search <command>`."""

from __future__ import annotations

import argparse
import logging
import statistics
import sys
import time

import psycopg
from pgvector.psycopg import register_vector

from tender_pipeline.cli import configure_logging, use_utf8_output
from tender_pipeline.config import ConfigError
from tender_pipeline.report import fmt, print_table
from tender_search import indexer
from tender_search import search as engine
from tender_search.config import load_search_settings
from tender_search.embedder import Embedder, make_embedder

log = logging.getLogger("tender_search")

# Realistic queries for `bench`: plain-language needs, product names, standards.
BENCH_QUERIES = (
    "fire safety equipment", "laptops for schools", "hospital medical equipment",
    "road construction work", "CCTV surveillance cameras", "office furniture chairs",
    "solar power plant", "drinking water purification", "IS 15683", "LED street lights",
    "ambulance hiring", "security manpower services", "printer toner cartridges",
    "steel pipes", "data centre servers", "uniform cloth for police",
    "electric cables 1.1 kV", "vehicle spare parts", "pesticides for agriculture",
    "software development services",
)  # fmt: skip


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tender_search", description="Hybrid semantic + keyword search over GeM tenders."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="enable pgvector and create the embeddings table")
    embed = commands.add_parser("embed", help="embed new and changed tenders")
    embed.add_argument("--rebuild", action="store_true", help="re-embed every tender")
    commands.add_parser("serve", help="run the HTTP API and search page")
    find = commands.add_parser("search", help="search from the terminal")
    find.add_argument("query")
    find.add_argument("--state")
    find.add_argument("--ministry")
    find.add_argument("--all", action="store_true", help="include closed bids")
    find.add_argument("--limit", type=int, default=10)
    bench = commands.add_parser("bench", help="measure search latency")
    bench.add_argument("--repeat", type=int, default=5, help="runs per query")
    return parser


def _timed_pass(conn: psycopg.Connection, embedder: Embedder) -> list[float]:
    timings = []
    for query in BENCH_QUERIES:
        started = time.perf_counter()
        engine.search(conn, embedder, query)
        timings.append((time.perf_counter() - started) * 1000)
        conn.rollback()  # end the transaction, as the API's pool does per request
    return timings


def _summary(label: str, timings: list[float]) -> str:
    ordered = sorted(timings)
    p95 = ordered[max(0, round(0.95 * len(ordered)) - 1)]
    return (
        f"{label}: {len(ordered)} searches, p50 {statistics.median(ordered):.1f} ms, "
        f"p95 {p95:.1f} ms, max {ordered[-1]:.1f} ms"
    )


def _bench(conn: psycopg.Connection, embedder: Embedder, repeat: int) -> None:
    engine.search(conn, embedder, "warm up")  # load the model and warm caches first
    conn.rollback()
    # First sight of each query pays for embedding it; repeats hit the query cache.
    print(_summary("new queries (embedding included)", _timed_pass(conn, embedder)))
    if repeat > 1:
        repeated = [t for _ in range(repeat - 1) for t in _timed_pass(conn, embedder)]
        print(_summary("repeated queries (embedding cached)", repeated))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    use_utf8_output()
    try:
        settings = load_search_settings()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    embedder = make_embedder(settings.embedding_model)

    if args.command == "serve":
        import uvicorn

        from tender_search.api import create_app

        uvicorn.run(
            create_app(settings.database_url, embedder), host=settings.host, port=settings.port
        )
        return 0

    try:
        conn = psycopg.connect(settings.database_url)
    except psycopg.OperationalError as exc:
        print(f"error: cannot connect to the database: {exc}", file=sys.stderr)
        return 2
    with conn:
        if args.command == "init-db":
            indexer.init_schema(conn)
            log.info("search schema is up to date")
            return 0
        register_vector(conn)
        if args.command == "embed":
            indexer.embed_pending(conn, embedder, settings.embed_batch_size, rebuild=args.rebuild)
            return 0
        if args.command == "search":
            filters = engine.Filters(args.state, args.ministry, open_only=not args.all)
            hits = engine.search(conn, embedder, args.query, filters, args.limit)
            print_table(
                sys.stdout,
                f'Results for "{args.query}"',
                ["bid number", "items", "buyer", "closes", "via"],
                [
                    (
                        h.bid_number,
                        (h.items or "")[:52],
                        h.buyer,
                        fmt(h.end_at),
                        "+".join(h.matched_by),
                    )
                    for h in hits
                ],
            )
            return 0
        if args.command == "bench":
            _bench(conn, embedder, args.repeat)
            return 0
    raise AssertionError(f"unhandled command {args.command!r}")
