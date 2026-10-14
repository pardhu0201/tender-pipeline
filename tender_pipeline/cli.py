"""Command-line entry point: `python -m tender_pipeline <command>`."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg

from tender_pipeline import db, pipeline
from tender_pipeline.alerts import send_alert
from tender_pipeline.config import ConfigError, Settings, load_settings
from tender_pipeline.export import FORMATS, export_tenders
from tender_pipeline.gem_client import GemClient
from tender_pipeline.normalize import SOURCE
from tender_pipeline.report import fmt, print_report, print_table

log = logging.getLogger("tender_pipeline")


def _positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tender_pipeline",
        description="Ingest Indian government tenders from GeM into PostgreSQL.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init-db", help="create or update the database schema (idempotent)")

    run = commands.add_parser("run", help="crawl GeM and upsert tenders")
    run.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="incremental: only new bids since the last run (default); full: the whole listing",
    )
    run.add_argument(
        "--max-pages", type=_positive_int, help="stop after this many pages (for trying it out)"
    )

    commands.add_parser("reprocess", help="re-derive all columns from stored raw records")

    status = commands.add_parser("status", help="show recent runs; optionally check freshness")
    status.add_argument(
        "--max-age-hours",
        type=float,
        help="exit 1 (and alert) if the last successful run finished longer ago than this",
    )

    commands.add_parser("report", help="print a summary of the stored tenders")

    export = commands.add_parser("export", help="write tenders as JSON Lines or CSV")
    export.add_argument("--format", choices=FORMATS, default="jsonl")
    export.add_argument("--open-only", action="store_true", help="only bids still open")
    export.add_argument("--output", type=Path, help="file to write (default: stdout)")
    return parser


def use_utf8_output() -> None:
    """Bid text can hold any Unicode (₹, Devanagari). On Windows, redirected
    output falls back to cp1252 and would crash on it, so always write UTF-8."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")


def stop_cleanly_on_sigterm() -> None:
    """`docker stop` and systemd send SIGTERM. Turn it into an exception so the
    run is recorded as terminated instead of being left 'running'."""

    def handler(signum: int, frame: object) -> None:
        raise pipeline.Terminated("terminated by SIGTERM")

    signal.signal(signal.SIGTERM, handler)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)


STATUS_COLUMNS = (
    "id", "mode", "status", "started", "took", "pages", "records", "new", "changed", "rej.",
    "coverage",
)  # fmt: skip


def _status_row(run: tuple) -> tuple:
    run_id, mode, status, started, took, pages, seen, new, changed, rejected, coverage = run[:11]
    shown_coverage = f"{coverage:.2%}" if coverage is not None else None
    return (
        run_id, mode, status, started, _minutes(took), pages, seen, new, changed, rejected,
        shown_coverage,
    )  # fmt: skip


def _status(conn: psycopg.Connection, settings: Settings, max_age_hours: float | None) -> int:
    runs = db.recent_runs(conn, SOURCE)
    print_table(sys.stdout, "Recent runs", list(STATUS_COLUMNS), [_status_row(r) for r in runs])
    for run_id, *_, error, issues in runs[:3]:
        if error:
            print(f"  run {run_id} error: {error}")
        for issue in issues:
            print(f"  run {run_id} issue: {issue}")

    if max_age_hours is None:
        return 0
    last = db.last_successful_run(conn, SOURCE)
    finished = last[1] if last else None
    if finished and datetime.now(UTC) - finished <= timedelta(hours=max_age_hours):
        print(f"\nFresh: the last successful run finished {fmt(finished)}.")
        return 0
    message = f"no successful run finished in the last {max_age_hours:g} hours"
    if finished:
        message += f" (the last one finished {fmt(finished)})"
    print(f"\nSTALE: {message}")
    send_alert(settings, "tender-pipeline data is stale", message)
    return 1


def _minutes(duration: timedelta | None) -> str | None:
    return f"{duration.total_seconds() / 60:.1f} min" if duration else None


def _export(conn: psycopg.Connection, args: argparse.Namespace) -> int:
    if args.output is None:
        rows = export_tenders(conn, sys.stdout, fmt=args.format, open_only=args.open_only)
    else:
        with args.output.open("w", encoding="utf-8", newline="") as out:
            rows = export_tenders(conn, out, fmt=args.format, open_only=args.open_only)
    log.info("exported %d tenders as %s", rows, args.format)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    use_utf8_output()
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)

    try:
        conn = db.connect(settings.database_url)
    except psycopg.OperationalError as exc:
        print(f"error: cannot connect to the database: {exc}", file=sys.stderr)
        return 2

    with conn:
        if args.command == "init-db":
            db.init_schema(conn)
            log.info("schema is up to date")
            return 0
        if args.command in ("run", "reprocess"):
            stop_cleanly_on_sigterm()
            try:
                if args.command == "run":
                    client = GemClient(settings)
                    pipeline.run(conn, client, settings, mode=args.mode, max_pages=args.max_pages)
                else:
                    counts = pipeline.reprocess(conn)
                    log.info(
                        "reprocessed %d rows: %d changed, %d no longer valid",
                        counts["checked"],
                        counts["changed"],
                        counts["now_invalid"],
                    )
            except pipeline.RunInProgress as exc:
                log.warning("%s", exc)  # not an error: the other run is doing the work
                return 0
            except pipeline.Terminated:
                return 143  # 128 + SIGTERM, the conventional exit status
            except KeyboardInterrupt:
                return 130
            except Exception:
                log.exception("%s failed", args.command)
                return 1
            return 0
        if args.command == "status":
            return _status(conn, settings, args.max_age_hours)
        if args.command == "report":
            print_report(conn)
            return 0
        if args.command == "export":
            return _export(conn, args)
    raise AssertionError(f"unhandled command {args.command!r}")
