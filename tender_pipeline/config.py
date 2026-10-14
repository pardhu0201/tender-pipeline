"""Runtime settings, read from environment variables.

A `.env` file in the working directory is loaded first, without overriding
variables that are already set, so local runs, cron and Docker share one
configuration format.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from tender_pipeline import __version__

# Identify the crawler honestly: the conventional "compatible" form keeps
# gateways happy while saying exactly what this is and where it lives.
DEFAULT_USER_AGENT = (
    f"Mozilla/5.0 (compatible; tender-pipeline/{__version__}; "
    "+https://github.com/pardhu0201/tender-pipeline)"
)

# Politeness floor: no configuration can make the crawler faster than this.
MIN_REQUEST_DELAY_SECONDS = 0.5

_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigError(RuntimeError):
    """A required setting is missing or malformed."""


@dataclass(frozen=True)
class Settings:
    database_url: str
    user_agent: str = DEFAULT_USER_AGENT
    request_delay_seconds: float = 1.0
    http_timeout_seconds: float = 30.0
    max_retries: int = 4
    incremental_overlap_hours: float = 6.0
    incremental_stop_after_pages: int = 3
    alert_webhook_url: str | None = None
    log_level: str = "INFO"


def load_dotenv(path: Path = Path(".env")) -> None:
    """Load KEY=VALUE lines from `path` into the environment (existing values win)."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _number(name: str, default: float, cast: type[int] | type[float]) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return cast(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from None


def load_settings(*, require_database: bool = True) -> Settings:
    load_dotenv()

    database_url = os.environ.get("DATABASE_URL", "").strip()
    if require_database and not database_url:
        raise ConfigError("DATABASE_URL is not set - copy .env.example to .env, or export it")

    delay = _number("REQUEST_DELAY_SECONDS", 1.0, float)
    if delay < MIN_REQUEST_DELAY_SECONDS:
        raise ConfigError(
            f"REQUEST_DELAY_SECONDS must be at least {MIN_REQUEST_DELAY_SECONDS} "
            "(this crawler is deliberately polite to a public server)"
        )

    max_retries = int(_number("MAX_RETRIES", 4, int))
    stop_after = int(_number("INCREMENTAL_STOP_AFTER_PAGES", 3, int))
    if max_retries < 0 or stop_after < 1:
        raise ConfigError("MAX_RETRIES must be >= 0 and INCREMENTAL_STOP_AFTER_PAGES >= 1")
    timeout = _number("HTTP_TIMEOUT_SECONDS", 30.0, float)
    overlap = _number("INCREMENTAL_OVERLAP_HOURS", 6.0, float)
    if timeout <= 0 or overlap < 0:
        raise ConfigError("HTTP_TIMEOUT_SECONDS must be > 0 and INCREMENTAL_OVERLAP_HOURS >= 0")

    log_level = os.environ.get("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    if log_level not in _LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {sorted(_LOG_LEVELS)}")

    return Settings(
        database_url=database_url,
        user_agent=os.environ.get("USER_AGENT", "").strip() or DEFAULT_USER_AGENT,
        request_delay_seconds=delay,
        http_timeout_seconds=timeout,
        max_retries=max_retries,
        incremental_overlap_hours=overlap,
        incremental_stop_after_pages=stop_after,
        alert_webhook_url=os.environ.get("ALERT_WEBHOOK_URL", "").strip() or None,
        log_level=log_level,
    )
