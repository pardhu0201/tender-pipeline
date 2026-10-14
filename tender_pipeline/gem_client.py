"""Client for the public bid listing on GeM (Government e-Marketplace).

GeM has no documented API. Its https://bidplus.gem.gov.in/all-bids page loads
results with an AJAX call, and this module replays exactly that request:

1. GET /all-bids. The response sets a session cookie and embeds a CSRF token
   (`csrf_bd_gem_nk`) in the page's JavaScript.
2. POST /all-bids-data with that cookie, the token, and a form field `payload`
   holding a JSON query (page number, filters, sort order).

The endpoint answers with a Solr-style document list, 10 records per page,
every field wrapped in a one-element list (normalize.py unwraps them).

Before any of that the client reads robots.txt and refuses to run if the
listing is disallowed, and it never sends more than one request per
`request_delay_seconds`.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from urllib.robotparser import RobotFileParser

import requests

from tender_pipeline.config import Settings

log = logging.getLogger(__name__)

BASE_URL = "https://bidplus.gem.gov.in"
ROBOTS_URL = f"{BASE_URL}/robots.txt"
LISTING_PAGE_URL = f"{BASE_URL}/all-bids"
LISTING_API_URL = f"{BASE_URL}/all-bids-data"
PAGE_SIZE = 10  # fixed by the server

# The product token robots.txt rules are matched against.
ROBOTS_AGENT = "tender-pipeline"

# Sort orders accepted by the endpoint - the values the page's own dropdown sends.
SORT_START_NEWEST = "Bid-Start-Date-Latest"
SORT_END_SOONEST = "Bid-End-Date-Oldest"

_TOKEN_RE = re.compile(r"csrf_bd_gem_nk['\"]\s*[:,]\s*['\"]([0-9a-fA-F]{16,128})['\"]")
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_OVERLOAD_STATUS = {429, 503}  # "slow down": halve our request rate for the rest of the run
_SESSION_STATUS = {401, 403, 419}  # 419: "session/CSRF expired" on PHP stacks
_BACKOFF_BASE_SECONDS = 2.0
_MAX_BACKOFF_SECONDS = 120.0
_MAX_REQUEST_DELAY_SECONDS = 30.0


class GemError(RuntimeError):
    """The listing could not be fetched."""


class GemSchemaError(GemError):
    """GeM answered, but not in the shape this client understands: the site changed."""


@dataclass(frozen=True)
class ListingPage:
    number: int  # 1-based page number
    total: int  # records the server says match the query
    docs: list[dict]


class _Retryable(Exception):
    def __init__(
        self, message: str, *, retry_after: float | None = None, reset_session: bool = False
    ) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.reset_session = reset_session


def extract_csrf_token(html: str) -> str | None:
    match = _TOKEN_RE.search(html)
    return match.group(1) if match else None


def build_query(page: int, sort: str) -> str:
    """The JSON query the all-bids page sends for an unfiltered list of ongoing bids."""
    query = {
        "page": page,
        "param": {"searchBid": "", "searchType": "fullText"},
        "filter": {
            "bidStatusType": "ongoing_bids",
            "byType": "all",
            "highBidValue": "",
            "byEndDate": {"from": "", "to": ""},
            "sort": sort,
        },
    }
    return json.dumps(query, separators=(",", ":"))


def parse_listing(body: dict, number: int) -> ListingPage:
    try:
        result = body["response"]["response"]
        total = int(result["numFound"])
        docs = result["docs"]
    except (KeyError, TypeError, ValueError) as exc:
        raise GemSchemaError(f"unexpected listing response shape ({exc!r})") from exc
    if not isinstance(docs, list):
        raise GemSchemaError("listing response 'docs' is not a list")
    return ListingPage(number=number, total=total, docs=docs)


def _retry_after_seconds(response: requests.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # HTTP-date form; fall back to normal backoff


class GemClient:
    def __init__(
        self,
        settings: Settings,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        self._settings = settings
        self._session = session if session is not None else requests.Session()
        self._session.headers["User-Agent"] = settings.user_agent
        self._sleep = sleep
        self._clock = clock
        self._rng = rng or random.Random()
        self._delay = settings.request_delay_seconds
        self._token: str | None = None
        self._robots_checked = False
        self._last_request_at: float | None = None
        self.requests_made = 0
        self.retries = 0
        self.session_refreshes = 0
        self.slowdowns = 0

    @property
    def http_metrics(self) -> dict[str, float]:
        """What the crawl cost the server, recorded with every run."""
        return {
            "requests": self.requests_made,
            "retries": self.retries,
            "session_refreshes": self.session_refreshes,
            "slowdowns": self.slowdowns,
            "request_delay_seconds": self._delay,
        }

    def iter_pages(
        self, sort: str = SORT_START_NEWEST, start_page: int = 1, max_pages: int | None = None
    ) -> Iterator[ListingPage]:
        """Yield pages until the listing ends (or `max_pages` have been fetched)."""
        number, fetched = start_page, 0
        while True:
            page = self.fetch_page(number, sort)
            fetched += 1
            yield page
            if not page.docs or number * PAGE_SIZE >= page.total:
                return
            if max_pages is not None and fetched >= max_pages:
                return
            number += 1

    def fetch_page(self, number: int, sort: str = SORT_START_NEWEST) -> ListingPage:
        attempts = self._settings.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                self._check_robots()
                if self._token is None:
                    self._bootstrap_session()
                return self._query(number, sort)
            except _Retryable as exc:
                if exc.reset_session:
                    self._reset_session()
                if attempt == attempts:
                    raise GemError(
                        f"page {number}: giving up after {attempts} attempts ({exc})"
                    ) from exc
                self.retries += 1
                delay = self._backoff_delay(attempt, exc.retry_after)
                log.warning(
                    "page %d: attempt %d/%d failed (%s) - retrying in %.1fs",
                    number,
                    attempt,
                    attempts,
                    exc,
                    delay,
                )
                self._sleep(delay)
        raise AssertionError("unreachable")

    # -- internals ---------------------------------------------------------

    def _check_robots(self) -> None:
        if self._robots_checked:
            return
        response = self._request("GET", ROBOTS_URL, allow_client_errors=True)
        if response.status_code < 400:  # a 4xx means "no robots.txt": everything allowed
            parser = RobotFileParser()
            parser.parse(response.text.splitlines())
            for url in (LISTING_PAGE_URL, LISTING_API_URL):
                if not parser.can_fetch(ROBOTS_AGENT, url):
                    raise GemError(f"robots.txt disallows {url} - refusing to crawl")
            crawl_delay = parser.crawl_delay(ROBOTS_AGENT)
            if crawl_delay:
                self._delay = max(self._delay, float(crawl_delay))
        self._robots_checked = True

    def _bootstrap_session(self) -> None:
        response = self._request("GET", LISTING_PAGE_URL)
        token = extract_csrf_token(response.text)
        if token is None:
            # Either the page layout changed or we got an interstitial; a fresh
            # session usually clears the latter, so let the retry loop decide.
            raise _Retryable("CSRF token not found on the listing page", reset_session=True)
        self._token = token
        log.debug("session bootstrapped")

    def _query(self, number: int, sort: str) -> ListingPage:
        response = self._request(
            "POST",
            LISTING_API_URL,
            data={"payload": build_query(number, sort), "csrf_bd_gem_nk": self._token},
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": LISTING_PAGE_URL,
            },
        )
        try:
            body = response.json()
        except ValueError:
            # HTML instead of JSON almost always means the session or token
            # expired mid-crawl. Start a new session and try again.
            raise _Retryable("response was not JSON", reset_session=True) from None
        if not isinstance(body, dict) or body.get("code") != 200:
            code = body.get("code") if isinstance(body, dict) else type(body).__name__
            raise _Retryable(f"API returned code {code!r}", reset_session=True)
        return parse_listing(body, number)

    def _request(
        self, method: str, url: str, *, allow_client_errors: bool = False, **kwargs
    ) -> requests.Response:
        self._throttle()
        self.requests_made += 1
        try:
            response = self._session.request(
                method, url, timeout=self._settings.http_timeout_seconds, **kwargs
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            raise _Retryable(f"{type(exc).__name__}: {exc}") from exc

        status = response.status_code
        if status in _OVERLOAD_STATUS:
            self._slow_down(status)
        if status in _RETRYABLE_STATUS:
            raise _Retryable(f"HTTP {status}", retry_after=_retry_after_seconds(response))
        if status in _SESSION_STATUS and not allow_client_errors:
            raise _Retryable(f"HTTP {status}", reset_session=True)
        if status >= 400 and not allow_client_errors:
            # Our request is wrong; repeating it will not help.
            raise GemError(f"HTTP {status} from {url}")
        return response

    def _slow_down(self, status: int) -> None:
        """The server says it is overloaded: halve our request rate for the rest of the run."""
        new_delay = min(self._delay * 2, _MAX_REQUEST_DELAY_SECONDS)
        if new_delay > self._delay:
            self.slowdowns += 1
            log.warning("HTTP %d from GeM - slowing to one request every %.1fs", status, new_delay)
            self._delay = new_delay

    def _throttle(self) -> None:
        if self._last_request_at is not None:
            wait = self._delay - (self._clock() - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._clock()

    def _backoff_delay(self, attempt: int, retry_after: float | None) -> float:
        # Exponential backoff with "equal jitter": half fixed, half random.
        ceiling = min(_MAX_BACKOFF_SECONDS, _BACKOFF_BASE_SECONDS * 2**attempt)
        delay = self._rng.uniform(ceiling / 2, ceiling)
        if retry_after is not None:
            delay = max(delay, min(retry_after, _MAX_BACKOFF_SECONDS))
        return delay

    def _reset_session(self) -> None:
        self.session_refreshes += 1
        self._token = None
        self._session.cookies.clear()
