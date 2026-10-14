"""CLI behaviour that needs no database (see test_pipeline_db.py for the rest)."""

import io
import runpy
import signal
import sys

import pytest

from tender_pipeline import pipeline
from tender_pipeline.cli import main, stop_cleanly_on_sigterm, use_utf8_output


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    for name in ("DATABASE_URL", "LOG_LEVEL", "REQUEST_DELAY_SECONDS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)  # never pick up a developer's real .env
    previous = signal.getsignal(signal.SIGTERM)
    yield
    signal.signal(signal.SIGTERM, previous)


def test_missing_database_url_is_a_clear_error(capsys):
    assert main(["status"]) == 2
    assert "DATABASE_URL is not set" in capsys.readouterr().err


def test_unreachable_database_is_a_clear_error(monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", "postgresql://nobody@127.0.0.1:1/none?connect_timeout=3")
    assert main(["status"]) == 2
    assert "cannot connect to the database" in capsys.readouterr().err


def test_usage_errors_exit_with_status_2():
    with pytest.raises(SystemExit) as excinfo:
        main(["run", "--max-pages", "0"])
    assert excinfo.value.code == 2


def test_python_dash_m_entry_point(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["tender_pipeline", "--help"])
    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("tender_pipeline", run_name="__main__")
    assert excinfo.value.code == 0
    assert "init-db" in capsys.readouterr().out


def test_output_is_utf8_even_on_a_narrow_console(monkeypatch):
    raw = io.BytesIO()
    monkeypatch.setattr("sys.stdout", io.TextIOWrapper(raw, encoding="cp1252"))
    use_utf8_output()
    print("Tender value ₹ 5 lakh, कार्य")
    sys.stdout.flush()
    assert raw.getvalue().decode("utf-8").strip() == "Tender value ₹ 5 lakh, कार्य"


def test_sigterm_becomes_a_recordable_exception():
    stop_cleanly_on_sigterm()
    handler = signal.getsignal(signal.SIGTERM)
    with pytest.raises(pipeline.Terminated, match="SIGTERM"):
        handler(signal.SIGTERM, None)
