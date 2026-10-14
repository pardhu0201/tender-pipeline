import json
import random

import pytest
import requests

from tender_pipeline.config import Settings
from tender_pipeline.gem_client import (
    LISTING_API_URL,
    LISTING_PAGE_URL,
    ROBOTS_URL,
    SORT_START_NEWEST,
    GemClient,
    GemError,
    GemSchemaError,
    build_query,
    extract_csrf_token,
)

TOKEN = "0123456789abcdef0123456789abcdef"
GEM_ROBOTS = "User-agent: *\nDisallow: /resources/\nDisallow: /bg_emd/epbgservice/PBGIntimation\n"


class FakeResponse:
    def __init__(self, status=200, *, json_body=None, text="", headers=None):
        self.status_code = status
        self._json = json_body
        self.text = json.dumps(json_body) if json_body is not None else text
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("not JSON")
        return self._json


class FakeCookies:
    def __init__(self):
        self.cleared = 0

    def clear(self):
        self.cleared += 1


class FakeSession:
    """Replays scripted responses and records every request made."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}
        self.cookies = FakeCookies()

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self.responses:
            raise AssertionError(f"unexpected request: {method} {url}")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeTime:
    def __init__(self):
        self.now = 1000.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def robots(text=GEM_ROBOTS):
    return FakeResponse(text=text)


def listing_page(html):
    return FakeResponse(text=html)


def api(docs, total=None):
    total = len(docs) if total is None else total
    return FakeResponse(
        json_body={"code": 200, "response": {"response": {"numFound": total, "docs": docs}}}
    )


def make_client(responses, **overrides):
    settings = Settings(database_url="", **({"max_retries": 3} | overrides))
    session = FakeSession(responses)
    clock = FakeTime()
    client = GemClient(
        settings, session=session, sleep=clock.sleep, clock=clock.clock, rng=random.Random(7)
    )
    return client, session, clock


def posted_form(call):
    _, _, kwargs = call
    return kwargs["data"]


# -- request shape ------------------------------------------------------------


def test_extract_csrf_token(listing_html):
    assert extract_csrf_token(listing_html) == TOKEN
    assert extract_csrf_token("<html><body>no token</body></html>") is None


def test_build_query_matches_what_the_page_sends():
    query = json.loads(build_query(3, SORT_START_NEWEST))
    assert query == {
        "page": 3,
        "param": {"searchBid": "", "searchType": "fullText"},
        "filter": {
            "bidStatusType": "ongoing_bids",
            "byType": "all",
            "highBidValue": "",
            "byEndDate": {"from": "", "to": ""},
            "sort": "Bid-Start-Date-Latest",
        },
    }


def test_fetch_page_replays_the_browser_flow(listing_html, listing_docs):
    client, session, _ = make_client([robots(), listing_page(listing_html), api(listing_docs)])

    page = client.fetch_page(1)

    assert page.number == 1 and page.total == 12 and len(page.docs) == 12
    assert [(m, u) for m, u, _ in session.calls] == [
        ("GET", ROBOTS_URL),
        ("GET", LISTING_PAGE_URL),
        ("POST", LISTING_API_URL),
    ]
    form = posted_form(session.calls[2])
    assert form["csrf_bd_gem_nk"] == TOKEN
    assert json.loads(form["payload"])["page"] == 1
    assert session.calls[2][2]["headers"]["X-Requested-With"] == "XMLHttpRequest"
    assert "tender-pipeline" in session.headers["User-Agent"]
    assert client.requests_made == 3


def test_session_and_robots_are_reused_across_pages(listing_html, listing_docs):
    client, session, _ = make_client(
        [robots(), listing_page(listing_html), api(listing_docs), api(listing_docs)]
    )
    client.fetch_page(1)
    client.fetch_page(2)
    assert [m for m, _, _ in session.calls] == ["GET", "GET", "POST", "POST"]
    assert json.loads(posted_form(session.calls[3])["payload"])["page"] == 2


# -- politeness ---------------------------------------------------------------------


def test_requests_are_spaced_out(listing_html, listing_docs):
    client, _, clock = make_client(
        [robots(), listing_page(listing_html), api(listing_docs), api(listing_docs)],
        request_delay_seconds=1.5,
    )
    client.fetch_page(1)
    client.fetch_page(2)
    # The fake clock only moves when we sleep, so every gap is a full delay.
    assert clock.sleeps == [1.5, 1.5, 1.5]


def test_robots_disallow_stops_the_crawl():
    client, session, _ = make_client([robots("User-agent: *\nDisallow: /all-bids\n")])
    with pytest.raises(GemError, match="robots.txt disallows"):
        client.fetch_page(1)
    assert len(session.calls) == 1


def test_robots_crawl_delay_slows_us_down(listing_html, listing_docs):
    client, _, clock = make_client(
        [robots("User-agent: *\nCrawl-delay: 5\n"), listing_page(listing_html), api(listing_docs)]
    )
    client.fetch_page(1)
    assert clock.sleeps == [5.0, 5.0]


def test_missing_robots_txt_means_allowed(listing_html, listing_docs):
    client, _, _ = make_client(
        [FakeResponse(404, text="Not Found"), listing_page(listing_html), api(listing_docs)]
    )
    assert len(client.fetch_page(1).docs) == 12


# -- failures and retries ------------------------------------------------------------


def test_server_errors_are_retried_with_backoff(listing_html, listing_docs):
    client, session, clock = make_client(
        [
            robots(),
            listing_page(listing_html),
            FakeResponse(503),
            FakeResponse(502),
            api(listing_docs),
        ]
    )
    assert len(client.fetch_page(1).docs) == 12
    backoffs = [s for s in clock.sleeps if s > 1.0]
    assert len(backoffs) == 2
    assert 2.0 <= backoffs[0] <= 4.0 and 4.0 <= backoffs[1] <= 8.0  # exponential, jittered
    assert client.requests_made == 5


def test_retry_after_is_respected(listing_html, listing_docs):
    client, _, clock = make_client(
        [
            robots(),
            listing_page(listing_html),
            FakeResponse(429, headers={"Retry-After": "30"}),
            api(listing_docs),
        ]
    )
    client.fetch_page(1)
    assert max(clock.sleeps) >= 30


def test_network_errors_are_retried(listing_html, listing_docs):
    client, _, _ = make_client(
        [robots(), listing_page(listing_html), requests.ConnectionError("reset"), api(listing_docs)]
    )
    assert len(client.fetch_page(1).docs) == 12


def test_expired_session_is_replaced(listing_html, listing_docs):
    new_token = "fedcba9876543210fedcba9876543210"
    client, session, _ = make_client(
        [
            robots(),
            listing_page(listing_html),
            FakeResponse(text="<html>session expired</html>"),  # HTML where JSON should be
            listing_page(listing_html.replace(TOKEN, new_token)),
            api(listing_docs),
        ]
    )
    assert len(client.fetch_page(1).docs) == 12
    assert session.cookies.cleared == 1
    assert posted_form(session.calls[-1])["csrf_bd_gem_nk"] == new_token


def test_api_error_code_starts_a_new_session(listing_html, listing_docs):
    client, session, _ = make_client(
        [
            robots(),
            listing_page(listing_html),
            FakeResponse(json_body={"code": 419, "message": "token mismatch"}),
            listing_page(listing_html),
            api(listing_docs),
        ]
    )
    client.fetch_page(1)
    assert session.cookies.cleared == 1


def test_client_errors_are_not_retried(listing_html):
    client, session, _ = make_client([robots(), listing_page(listing_html), FakeResponse(404)])
    with pytest.raises(GemError, match="HTTP 404"):
        client.fetch_page(1)
    assert len(session.calls) == 3


def test_changed_response_shape_fails_fast(listing_html):
    client, session, _ = make_client(
        [robots(), listing_page(listing_html), FakeResponse(json_body={"code": 200, "data": []})]
    )
    with pytest.raises(GemSchemaError):
        client.fetch_page(1)
    assert len(session.calls) == 3


def test_gives_up_after_max_retries(listing_html):
    client, session, _ = make_client(
        [robots(), listing_page(listing_html)] + [FakeResponse(503)] * 3, max_retries=2
    )
    with pytest.raises(GemError, match="giving up after 3 attempts"):
        client.fetch_page(1)
    assert len(session.calls) == 5


def test_missing_token_eventually_fails(listing_html):
    no_token = listing_html.replace(TOKEN, "")
    client, _, _ = make_client([robots()] + [listing_page(no_token)] * 3, max_retries=2)
    with pytest.raises(GemError, match="CSRF token not found"):
        client.fetch_page(1)


def test_forbidden_starts_a_new_session(listing_html, listing_docs):
    client, session, _ = make_client(
        [robots(), listing_page(listing_html), FakeResponse(403), listing_page(listing_html)]
        + [api(listing_docs)]
    )
    assert len(client.fetch_page(1).docs) == 12
    assert session.cookies.cleared == 1


def test_retry_after_as_a_date_falls_back_to_backoff(listing_html, listing_docs):
    date_form = FakeResponse(502, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    client, _, clock = make_client(
        [robots(), listing_page(listing_html), date_form, api(listing_docs)]
    )
    client.fetch_page(1)
    assert max(clock.sleeps) <= 4.0  # the first backoff step, not a parsed date


def test_docs_that_are_not_a_list_fail_fast(listing_html):
    body = {"code": 200, "response": {"response": {"numFound": 1, "docs": {"id": 1}}}}
    client, _, _ = make_client([robots(), listing_page(listing_html), FakeResponse(json_body=body)])
    with pytest.raises(GemSchemaError, match="not a list"):
        client.fetch_page(1)


# -- adaptive politeness and metrics --------------------------------------------------------


def test_overload_halves_our_request_rate(listing_html, listing_docs):
    client, _, clock = make_client(
        [robots(), listing_page(listing_html), FakeResponse(429), api(listing_docs)]
        + [api(listing_docs)]
    )
    client.fetch_page(1)
    client.fetch_page(2)
    assert client.http_metrics["slowdowns"] == 1
    assert client.http_metrics["request_delay_seconds"] == 2.0
    assert clock.sleeps[-1] == 2.0  # the gap before page 2 doubled from 1s


def test_slowdowns_are_capped(listing_html, listing_docs):
    client, _, _ = make_client(
        [robots(), listing_page(listing_html)] + [FakeResponse(503)] * 6 + [api(listing_docs)],
        max_retries=6,
        request_delay_seconds=10.0,
    )
    client.fetch_page(1)
    assert client.http_metrics["request_delay_seconds"] == 30.0
    assert client.http_metrics["slowdowns"] == 2  # 10 -> 20 -> 30, then no further


def test_http_metrics_count_retries_and_session_refreshes(listing_html, listing_docs):
    client, _, _ = make_client(
        [robots(), listing_page(listing_html), FakeResponse(text="<html>expired</html>")]
        + [listing_page(listing_html), api(listing_docs)]
    )
    client.fetch_page(1)
    assert client.http_metrics == {
        "requests": 5,
        "retries": 1,
        "session_refreshes": 1,
        "slowdowns": 0,
        "request_delay_seconds": 1.0,
    }


# -- pagination ---------------------------------------------------------------------------


def test_iter_pages_stops_after_the_last_page(listing_html, listing_docs):
    client, session, _ = make_client(
        [robots(), listing_page(listing_html)]
        + [api(listing_docs[:10], total=25)] * 2
        + [api(listing_docs[:5], total=25)]
    )
    pages = list(client.iter_pages())
    assert [p.number for p in pages] == [1, 2, 3]
    assert len(session.calls) == 5


def test_iter_pages_respects_max_pages(listing_html, listing_docs):
    client, _, _ = make_client(
        [robots(), listing_page(listing_html)] + [api(listing_docs[:10], total=1000)] * 2
    )
    assert [p.number for p in client.iter_pages(max_pages=2)] == [1, 2]


def test_iter_pages_stops_on_an_empty_page(listing_html):
    client, _, _ = make_client([robots(), listing_page(listing_html), api([], total=40)])
    assert len(list(client.iter_pages())) == 1
