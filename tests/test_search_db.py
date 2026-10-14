"""Search against a real PostgreSQL with pgvector.

Like the pipeline's database tests, these DROP AND RECREATE tables, so they only
run when TEST_DATABASE_URL is set; and they skip if pgvector is not installed.
They use the deterministic hashing embedder, so no model is ever downloaded.
"""

import copy
import os
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient
from pgvector.psycopg import register_vector

from tender_pipeline import db
from tender_pipeline.normalize import IST, normalize
from tender_search import indexer
from tender_search import search as engine
from tender_search.api import create_app
from tender_search.cli import main
from tender_search.embedder import HashingEmbedder

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL, reason="set TEST_DATABASE_URL to run database tests"
)

FIRE = "GEM/2026/B/8114345"  # Portable Fire Extinguishers, Odisha
LAMPS = "GEM/2026/B/8114297"  # LED lamps, Department of Panchayati Raj


def gem_time(moment: datetime) -> str:
    return moment.astimezone(IST).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"


def reopened(doc: dict, closes_in: timedelta) -> dict:
    """The fixture's real bids have closed by now; move their window around today."""
    doc = copy.deepcopy(doc)
    now = datetime.now(UTC)
    doc["final_start_date_sort"] = [gem_time(now - timedelta(days=5))]
    doc["final_end_date_sort"] = [gem_time(now + closes_in)]
    return doc


@pytest.fixture
def conn(listing_docs):
    with psycopg.connect(TEST_DATABASE_URL) as connection:
        connection.execute(
            "DROP TABLE IF EXISTS tender_embeddings, rejected_records, tenders, scrape_runs CASCADE"
        )
        connection.commit()
        db.init_schema(connection)
        try:
            indexer.init_schema(connection)
        except psycopg.Error as exc:
            if "vector" not in str(exc):
                raise
            pytest.skip("pgvector is not installed in the test database")

        docs = [reopened(doc, timedelta(days=10)) for doc in listing_docs]
        # One bid closing within a day, one already closed, for the time filters.
        docs[0] = reopened(listing_docs[0], timedelta(hours=12))
        docs[1] = reopened(listing_docs[1], timedelta(days=-1))
        docs[1]["final_start_date_sort"] = [gem_time(datetime.now(UTC) - timedelta(days=9))]
        run_id = db.start_run(connection, "gem", "full")
        db.upsert_tenders(connection, [normalize(doc) for doc in docs], run_id)
        connection.commit()
        register_vector(connection)
        yield connection


@pytest.fixture
def embedded(conn):
    indexer.embed_pending(conn, HashingEmbedder(), batch_size=5)
    return conn


def numbers(hits):
    return [hit.bid_number for hit in hits]


# -- indexing --------------------------------------------------------------------


def test_embedding_is_incremental(conn):
    embedder = HashingEmbedder()
    assert indexer.embed_pending(conn, embedder, batch_size=5).embedded == 12  # 3 batches
    assert indexer.embed_pending(conn, embedder).embedded == 0  # nothing changed

    conn.execute("UPDATE tenders SET content_hash = 'changed' WHERE bid_number = %s", (FIRE,))
    conn.commit()
    assert indexer.embed_pending(conn, embedder).embedded == 1  # only the changed tender

    assert indexer.embed_pending(conn, embedder, rebuild=True).embedded == 12


def test_a_new_model_re_embeds_everything(embedded):
    class NewModel(HashingEmbedder):
        name = "hashing-v2"

    assert indexer.embed_pending(embedded, NewModel()).embedded == 12
    models = embedded.execute("SELECT DISTINCT model FROM tender_embeddings").fetchall()
    assert models == [("hashing-v2",)]


def test_schema_is_idempotent(embedded):
    indexer.init_schema(embedded)
    assert embedded.execute("SELECT count(*) FROM tender_embeddings").fetchone()[0] == 12


# -- search ----------------------------------------------------------------------


def test_both_retrievers_agree_on_the_obvious_match(embedded):
    hits = engine.search(embedded, HashingEmbedder(), "fire extinguishers")
    assert hits[0].bid_number == FIRE
    assert hits[0].matched_by == ("semantic", "keyword")
    assert hits[0].state == "Odisha" and hits[0].buyer == "Odisha"


def test_scores_are_descending_and_limit_is_respected(embedded):
    hits = engine.search(embedded, HashingEmbedder(), "lamps cable steel", limit=3)
    assert len(hits) <= 3
    assert [h.score for h in hits] == sorted((h.score for h in hits), reverse=True)


def test_filters(embedded, listing_docs):
    embedder = HashingEmbedder()
    query = "supply of items"

    assert {
        h.state for h in engine.search(embedded, embedder, query, engine.Filters(state="odisha"))
    } == {"Odisha"}

    defence = engine.search(embedded, embedder, query, engine.Filters(ministry="defence"))
    assert defence and all(h.ministry == "Ministry of Defence" for h in defence)

    closed = listing_docs[1]["b_bid_number"][0]
    assert closed not in numbers(
        engine.search(embedded, embedder, query, engine.Filters(), limit=50)
    )
    everything = engine.search(embedded, embedder, query, engine.Filters(open_only=False), limit=50)
    assert closed in numbers(everything)

    soon = engine.search(
        embedded, embedder, query, engine.Filters(closing_within_hours=24), limit=50
    )
    assert numbers(soon) == [listing_docs[0]["b_bid_number"][0]]


def test_blank_query_returns_nothing(embedded):
    assert engine.search(embedded, HashingEmbedder(), "   ") == []


def test_similar_tenders(embedded):
    hits = engine.similar(embedded, LAMPS, limit=5)
    assert hits and LAMPS not in numbers(hits)
    assert all(h.matched_by == ("semantic",) for h in hits)
    assert [h.score for h in hits] == sorted((h.score for h in hits), reverse=True)
    assert engine.similar(embedded, "GEM/0000/B/0") is None


def test_facets_count_open_bids(embedded):
    facets = engine.facets(embedded)
    assert dict(facets["state"])["Odisha"] == 1
    assert sum(count for _, count in facets["ministry"]) <= 11  # the closed bid is excluded


# -- HTTP API ------------------------------------------------------------------------


@pytest.fixture
def client(embedded):
    with TestClient(create_app(TEST_DATABASE_URL, HashingEmbedder(), pool_size=2)) as test_client:
        yield test_client


def test_api_search(client):
    response = client.get("/search", params={"q": "fire extinguishers", "limit": 3})
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == len(body["results"]) <= 3
    top = body["results"][0]
    assert top["bid_number"] == FIRE
    assert top["closes"].endswith("+05:30")
    assert top["matched_by"] == ["semantic", "keyword"]


def test_api_validation_and_errors(client):
    assert client.get("/search", params={"q": ""}).status_code == 422
    assert client.get("/search", params={"q": "x", "limit": 500}).status_code == 422
    assert client.get("/similar", params={"bid": "GEM/0000/B/0"}).status_code == 404


def test_api_similar_facets_health_and_page(client):
    assert client.get("/similar", params={"bid": LAMPS}).status_code == 200
    assert "Odisha" in [f["value"] for f in client.get("/facets").json()["state"]]
    assert client.get("/health").json() == {
        "status": "ok", "tenders": 12, "embedded": 12, "model": "hashing-384",
    }  # fmt: skip
    page = client.get("/")
    assert page.status_code == 200 and "<title>Tender Search</title>" in page.text


# -- CLI ---------------------------------------------------------------------------------


def test_cli(conn, monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    monkeypatch.setenv("EMBEDDING_MODEL", "hashing-384")
    assert main(["init-db"]) == 0
    assert main(["embed"]) == 0
    assert main(["search", "fire extinguishers", "--limit", "2"]) == 0
    assert FIRE in capsys.readouterr().out
    assert main(["bench", "--repeat", "2"]) == 0
    out = capsys.readouterr().out
    assert "new queries" in out and "repeated queries" in out and "p50" in out
