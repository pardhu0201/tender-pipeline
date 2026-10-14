"""Text embedders: a real ONNX model for production, a hashing one for tests.

Both produce 384-dimensional vectors, matching `vector(384)` in the schema.
"""

from __future__ import annotations

import hashlib
import re
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Protocol

import numpy as np

DIMENSIONS = 384
# Texts per model call. A transformer pads every text in a batch to the longest
# one, and attention memory grows with batch x length^2: 256 tender texts of
# ~300 tokens took 6 GB of RAM. 32 keeps memory small and padding waste low.
INFERENCE_BATCH = 32
_WORD_RE = re.compile(r"[a-z0-9]+")


class Embedder(Protocol):
    name: str

    def embed_documents(self, texts: Sequence[str]) -> list[np.ndarray]: ...

    def embed_query(self, text: str) -> np.ndarray: ...


class FastEmbedEmbedder:
    """Sentence embeddings via fastembed (ONNX runtime): no PyTorch, small enough
    for a 512 MB container. The model is loaded on first use, from the local
    cache when it is there (the Docker image bakes it in at build time)."""

    def __init__(self, model_name: str, query_cache_size: int = 1024) -> None:
        self.name = model_name
        self._model: Any = None  # fastembed.TextEmbedding, once loaded
        # People repeat searches (and a demo repeats the example ones), so the
        # last few query vectors are kept: a repeat skips the model entirely.
        self._query_cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self._query_cache_size = query_cache_size
        self._lock = threading.Lock()  # the API calls this from a thread pool

    def _load(self) -> Any:
        if self._model is None:
            from fastembed import TextEmbedding  # imported lazily: heavy, and unused in tests

            self._model = TextEmbedding(self.name)
        return self._model

    def embed_documents(self, texts: Sequence[str]) -> list[np.ndarray]:
        model = self._load()
        return by_length(
            texts, lambda batch: list(model.passage_embed(batch, batch_size=INFERENCE_BATCH))
        )

    def embed_query(self, text: str) -> np.ndarray:
        key = " ".join(text.lower().split())
        with self._lock:
            if key in self._query_cache:
                self._query_cache.move_to_end(key)
                return self._query_cache[key]
        # bge models expect queries and passages to be encoded differently;
        # query_embed adds the instruction prefix the model was trained with.
        vector = next(iter(self._load().query_embed(text)))
        vector.flags.writeable = False  # shared by every later hit on this key
        with self._lock:
            self._query_cache[key] = vector
            if len(self._query_cache) > self._query_cache_size:
                self._query_cache.popitem(last=False)  # least recently used
        return vector


def by_length(
    texts: Sequence[str], embed: Callable[[list[str]], list[np.ndarray]]
) -> list[np.ndarray]:
    """Embed texts sorted by length (so each model batch pads to a similar
    length), then return the vectors in the original order."""
    order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
    vectors = embed([texts[i] for i in order])
    result: list[np.ndarray] = [vectors[0]] * len(texts) if texts else []
    for position, index in enumerate(order):
        result[index] = vectors[position]
    return result


class HashingEmbedder:
    """Deterministic bag-of-words embedding (the "hashing trick").

    Not semantic, but texts that share words land close together, which is
    enough to test ranking, filtering and fusion without downloading a model.
    """

    name = "hashing-384"

    def embed_documents(self, texts: Sequence[str]) -> list[np.ndarray]:
        return [self._embed(text) for text in texts]

    def embed_query(self, text: str) -> np.ndarray:
        return self._embed(text)

    @staticmethod
    def _embed(text: str) -> np.ndarray:
        vector = np.zeros(DIMENSIONS, dtype=np.float32)
        for word in _words(text):
            digest = hashlib.blake2b(word.encode(), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "little") % DIMENSIONS
            vector[bucket] += 1.0 if digest[4] % 2 else -1.0
        norm = float(np.linalg.norm(vector))
        return vector / norm if norm else vector


def _words(text: str) -> Iterable[str]:
    return _WORD_RE.findall(text.lower())


def make_embedder(model_name: str) -> Embedder:
    if model_name == HashingEmbedder.name:
        return HashingEmbedder()
    return FastEmbedEmbedder(model_name)
