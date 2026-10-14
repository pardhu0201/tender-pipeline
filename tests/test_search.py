"""Search logic that needs no database (see test_search_db.py for the rest)."""

import numpy as np
import pytest

from tender_pipeline.config import ConfigError
from tender_search.cli import main
from tender_search.config import DEFAULT_MODEL, load_search_settings
from tender_search.embedder import (
    DIMENSIONS,
    INFERENCE_BATCH,
    FastEmbedEmbedder,
    HashingEmbedder,
    by_length,
    make_embedder,
)
from tender_search.indexer import document_text
from tender_search.search import Filters, filter_clause, reciprocal_rank_fusion

# -- reciprocal rank fusion ------------------------------------------------------


def test_rrf_rewards_agreement_between_retrievers():
    fused = reciprocal_rank_fusion({"semantic": [1, 2, 3], "keyword": [3, 4]})
    assert [item for item, _, _ in fused] == [3, 1, 2, 4]
    assert fused[0] == (3, pytest.approx(1 / 63 + 1 / 61), ("semantic", "keyword"))


def test_rrf_scores_by_rank_not_raw_score():
    fused = {item: score for item, score, _ in reciprocal_rank_fusion({"a": [7, 8]}, k=60)}
    assert fused == {7: pytest.approx(1 / 61), 8: pytest.approx(1 / 62)}


def test_rrf_breaks_ties_deterministically():
    fused = reciprocal_rank_fusion({"semantic": [5], "keyword": [2]})
    assert [item for item, _, _ in fused] == [2, 5]  # equal scores: lower id first


def test_rrf_of_nothing_is_nothing():
    assert reciprocal_rank_fusion({"semantic": [], "keyword": []}) == []


# -- filters ---------------------------------------------------------------------


def test_default_filter_is_open_bids_only():
    where, params = filter_clause(Filters())
    assert "t.end_at > now()" in where and params == {}


def test_all_filters_are_parameterised():
    where, params = filter_clause(
        Filters(state=" Odisha ", ministry="Defence", open_only=False, closing_within_hours=48)
    )
    assert "end_at > now()" not in where
    assert params == {"state": "Odisha", "ministry": "%Defence%", "closing_hours": 48}
    assert "Odisha" not in where  # values travel as parameters, never inside the SQL


def test_like_wildcards_in_user_input_are_escaped():
    _, params = filter_clause(Filters(ministry="100%_sure"))
    assert params["ministry"] == "%100\\%\\_sure%"


# -- embedding ------------------------------------------------------------------


def test_hashing_embedder_is_deterministic_and_normalised():
    embedder = HashingEmbedder()
    first = embedder.embed_query("Portable Fire Extinguishers")
    assert first.shape == (DIMENSIONS,)
    assert np.allclose(first, embedder.embed_documents(["portable fire extinguishers"])[0])
    assert np.linalg.norm(first) == pytest.approx(1.0)
    assert not np.any(embedder.embed_query("!!!"))  # nothing to hash: a zero vector


def test_hashing_embedder_puts_shared_words_close_together():
    embedder = HashingEmbedder()
    query = embedder.embed_query("fire extinguishers")
    near = embedder.embed_query("Portable Fire Extinguishers ISI Marked")
    far = embedder.embed_query("High End Laptop Notebook")
    assert float(query @ near) > float(query @ far)


def test_document_text_names_the_buyer():
    text = document_text("LED lamps", None, "Department of Panchayati Raj", "Odisha")
    assert text == "LED lamps. Buyer: Department of Panchayati Raj, Odisha"
    assert document_text("LED lamps", None, None, None) == "LED lamps"


def test_document_text_drops_repeated_items():
    repeated = "Taxi - Sedan; Local,Taxi - Sedan; Local, taxi - sedan; local,Taxi - SUV,,"
    assert document_text(repeated, None, None, None) == "Taxi - Sedan; Local, Taxi - SUV"


def test_by_length_batches_short_texts_together_but_keeps_order():
    texts = ["a much longer tender description", "short", "medium text"]
    seen = []

    def fake_model(batch):
        seen.append(list(batch))
        return [np.array([len(text)], dtype=np.float32) for text in batch]

    vectors = by_length(texts, fake_model)
    assert seen == [["short", "medium text", "a much longer tender description"]]
    assert [int(v[0]) for v in vectors] == [len(t) for t in texts]  # original order restored
    assert by_length([], fake_model) == []


class FakeOnnxModel:
    """Stands in for fastembed.TextEmbedding: records calls, returns fixed vectors."""

    def __init__(self):
        self.query_calls = 0
        self.batches = []

    def passage_embed(self, texts, batch_size):
        self.batches.append((list(texts), batch_size))
        return (np.full(DIMENSIONS, len(text), dtype=np.float32) for text in texts)

    def query_embed(self, text):
        self.query_calls += 1
        return iter([np.full(DIMENSIONS, 1.0, dtype=np.float32)])


def fast_embedder(cache_size=1024):
    embedder = FastEmbedEmbedder(DEFAULT_MODEL, query_cache_size=cache_size)
    embedder._model = FakeOnnxModel()  # skip the download; test our logic only
    return embedder


def test_fastembed_documents_are_batched_by_length():
    embedder = fast_embedder()
    vectors = embedder.embed_documents(["longest text", "a", "mid"])
    assert [float(v[0]) for v in vectors] == [12.0, 1.0, 3.0]
    assert embedder._model.batches == [(["a", "mid", "longest text"], INFERENCE_BATCH)]


def test_repeated_queries_skip_the_model():
    embedder = fast_embedder()
    first = embedder.embed_query("Fire  Extinguishers")
    again = embedder.embed_query("fire extinguishers")  # same query, different spacing/case
    assert again is first and embedder._model.query_calls == 1
    assert not first.flags.writeable  # cached vectors cannot be mutated by a caller


def test_query_cache_evicts_the_least_recently_used():
    embedder = fast_embedder(cache_size=2)
    embedder.embed_query("a")
    embedder.embed_query("b")
    embedder.embed_query("a")  # refreshes "a"
    embedder.embed_query("c")  # evicts "b"
    assert list(embedder._query_cache) == ["a", "c"]
    embedder.embed_query("b")
    assert embedder._model.query_calls == 4


def test_make_embedder_picks_the_test_embedder_by_name():
    assert isinstance(make_embedder("hashing-384"), HashingEmbedder)
    assert make_embedder(DEFAULT_MODEL).name == DEFAULT_MODEL  # not loaded until first use


# -- settings ------------------------------------------------------------------------


def test_search_settings(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name in ("DATABASE_URL", "EMBEDDING_MODEL", "EMBED_BATCH_SIZE", "API_PORT"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ConfigError):
        load_search_settings()

    monkeypatch.setenv("DATABASE_URL", "postgresql://x@localhost/db")
    settings = load_search_settings()
    assert (settings.embedding_model, settings.port) == (DEFAULT_MODEL, 8000)

    monkeypatch.setenv("EMBED_BATCH_SIZE", "0")
    with pytest.raises(ConfigError):
        load_search_settings()


def test_cli_errors_are_clear(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert main(["embed"]) == 2
    assert "DATABASE_URL is not set" in capsys.readouterr().err

    monkeypatch.setenv("DATABASE_URL", "postgresql://nobody@127.0.0.1:1/none?connect_timeout=3")
    monkeypatch.setenv("EMBEDDING_MODEL", "hashing-384")
    assert main(["embed"]) == 2
    assert "cannot connect to the database" in capsys.readouterr().err


def test_python_dash_m_entry_point(monkeypatch, capsys):
    import runpy

    monkeypatch.setattr("sys.argv", ["tender_search", "--help"])
    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module("tender_search", run_name="__main__")
    assert excinfo.value.code == 0
    assert "embed" in capsys.readouterr().out
