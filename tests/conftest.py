import copy
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def listing_body() -> dict:
    """A real GeM listing response: 12 records chosen to cover every edge case seen."""
    return json.loads((FIXTURES / "gem_listing_page.json").read_text(encoding="utf-8"))


@pytest.fixture
def listing_docs(listing_body) -> list[dict]:
    return listing_body["response"]["response"]["docs"]


@pytest.fixture
def listing_html() -> str:
    return (FIXTURES / "gem_all_bids_snippet.html").read_text(encoding="utf-8")


@pytest.fixture
def fixture_doc(listing_docs):
    """Look up a fixture record by bid number; returns a deep copy, safe to mutate."""

    def get(bid_number: str) -> dict:
        for doc in listing_docs:
            if doc["b_bid_number"] == [bid_number]:
                return copy.deepcopy(doc)
        raise KeyError(bid_number)

    return get
