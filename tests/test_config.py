import pytest

from tender_pipeline.config import DEFAULT_USER_AGENT, ConfigError, load_settings

ENV_VARS = (
    "DATABASE_URL", "REQUEST_DELAY_SECONDS", "MAX_RETRIES", "LOG_LEVEL", "USER_AGENT",
    "ALERT_WEBHOOK_URL", "INCREMENTAL_STOP_AFTER_PAGES", "HTTP_TIMEOUT_SECONDS",
    "INCREMENTAL_OVERLAP_HOURS",
)  # fmt: skip


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)  # never pick up a developer's real .env


def test_defaults(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x@localhost/db")
    settings = load_settings()
    assert settings.request_delay_seconds == 1.0
    assert settings.user_agent == DEFAULT_USER_AGENT
    assert settings.alert_webhook_url is None


def test_database_url_is_required():
    with pytest.raises(ConfigError, match="DATABASE_URL"):
        load_settings()


def test_dotenv_is_loaded_but_never_overrides_the_environment(monkeypatch, tmp_path):
    (tmp_path / ".env").write_text(
        '# comment\nDATABASE_URL="postgresql://from-file/db"\nexport LOG_LEVEL=debug\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    settings = load_settings()
    assert settings.database_url == "postgresql://from-file/db"
    assert settings.log_level == "WARNING"


def test_politeness_floor_cannot_be_configured_away(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x@localhost/db")
    monkeypatch.setenv("REQUEST_DELAY_SECONDS", "0.1")
    with pytest.raises(ConfigError, match="at least 0.5"):
        load_settings()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("MAX_RETRIES", "lots"),
        ("LOG_LEVEL", "LOUD"),
        ("INCREMENTAL_STOP_AFTER_PAGES", "0"),
        ("HTTP_TIMEOUT_SECONDS", "0"),
        ("INCREMENTAL_OVERLAP_HOURS", "-1"),
    ],
)
def test_bad_values_are_reported(monkeypatch, name, value):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x@localhost/db")
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigError):
        load_settings()
