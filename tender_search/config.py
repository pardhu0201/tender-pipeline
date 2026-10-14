"""Settings for the search service, read from the environment (and .env)."""

from __future__ import annotations

import os
from dataclasses import dataclass

from tender_pipeline.config import ConfigError, load_dotenv

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"  # 384 dimensions, ~130 MB as ONNX


@dataclass(frozen=True)
class SearchSettings:
    database_url: str
    embedding_model: str = DEFAULT_MODEL
    embed_batch_size: int = 256
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"


def load_search_settings() -> SearchSettings:
    load_dotenv()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise ConfigError("DATABASE_URL is not set - copy .env.example to .env, or export it")
    try:
        batch = int(os.environ.get("EMBED_BATCH_SIZE", "256"))
        port = int(os.environ.get("API_PORT", "8000"))
    except ValueError as exc:
        raise ConfigError(f"EMBED_BATCH_SIZE and API_PORT must be integers ({exc})") from None
    if batch < 1:
        raise ConfigError("EMBED_BATCH_SIZE must be at least 1")
    return SearchSettings(
        database_url=database_url,
        embedding_model=os.environ.get("EMBEDDING_MODEL", "").strip() or DEFAULT_MODEL,
        embed_batch_size=batch,
        host=os.environ.get("API_HOST", "").strip() or "127.0.0.1",
        port=port,
        log_level=os.environ.get("LOG_LEVEL", "INFO").strip().upper() or "INFO",
    )
