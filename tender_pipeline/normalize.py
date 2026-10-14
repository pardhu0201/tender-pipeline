"""Turn raw GeM listing records into clean, validated `Tender` rows.

Everything here is a pure function of the raw record. That is what makes
`reprocess` possible: fix a rule, then re-derive every stored row from the raw
payload kept in Postgres instead of re-crawling the site.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from tender_pipeline.gem_client import BASE_URL

SOURCE = "gem"

# India Standard Time is a fixed UTC+05:30 with no daylight saving, so a fixed
# offset is exact (and avoids needing the tz database on Windows).
IST = timezone(timedelta(hours=5, minutes=30), name="IST")

# Dropped before a record is stored. `b.b_created_by` is the GeM account ID of
# the official who created the bid: nothing downstream needs it.
DROPPED_FIELDS = ("b.b_created_by",)

BID_TYPES = {1: "bid", 2: "reverse_auction", 5: "direct_reverse_auction"}

_BID_NUMBER_RE = re.compile(r"GEM/\d{4}/[BR]/\d+")
_PLACEHOLDERS = {"", "na", "n/a", "n.a.", "nil", "null", "none", "-", "--"}

# State-government buyers carry no ministry; their state is the suffix of the
# department name, e.g. "Energy Department Uttar Pradesh".
STATES_AND_UTS = (
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa",
    "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala",
    "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", "Mizoram", "Nagaland",
    "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Andaman and Nicobar Islands",
    "Chandigarh", "Dadra and Nagar Haveli and Daman and Diu", "Delhi",
    "Jammu and Kashmir", "Ladakh", "Lakshadweep", "Puducherry",
)  # fmt: skip
_STATE_ALIASES = {
    "orissa": "Odisha",
    "pondicherry": "Puducherry",
    "uttaranchal": "Uttarakhand",
    "jammu & kashmir": "Jammu and Kashmir",
    "andaman & nicobar islands": "Andaman and Nicobar Islands",
    "andaman and nicobar": "Andaman and Nicobar Islands",
    "dadra and nagar haveli": "Dadra and Nagar Haveli and Daman and Diu",
    "daman and diu": "Dadra and Nagar Haveli and Daman and Diu",
    "nct of delhi": "Delhi",
}
# Longest first, so "Dadra and Nagar Haveli and Daman and Diu" beats "Daman and Diu".
_STATE_SUFFIXES = sorted(
    [(name.lower(), name) for name in STATES_AND_UTS] + list(_STATE_ALIASES.items()),
    key=lambda pair: len(pair[0]),
    reverse=True,
)


class RecordRejected(ValueError):
    """A record failed validation. `reason` is a short machine-readable code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Tender:
    source: str
    source_id: str
    bid_number: str
    bid_type: str
    parent_bid_number: str | None
    items: str | None
    items_detail: str | None
    quantity: int | None
    ministry: str | None
    department: str | None
    state: str | None
    start_at: datetime
    end_at: datetime
    is_high_value: bool
    is_single_packet: bool
    is_global_tender: bool
    category_ids: tuple[str, ...]
    document_url: str
    raw: dict[str, Any]
    content_hash: str


def unwrap(doc: dict[str, Any], key: str) -> Any:
    """GeM wraps every value in a one-element list; return the bare value."""
    value = doc.get(key)
    if isinstance(value, list):
        return value[0] if value else None
    return value


def clean_text(value: Any) -> str | None:
    """Collapse whitespace; map empty strings and placeholders like "NA" to None."""
    if value is None:
        return None
    text = " ".join(str(value).split())
    return None if text.lower() in _PLACEHOLDERS else text


def parse_gem_timestamp(value: Any) -> datetime:
    """Parse a GeM timestamp into an aware datetime.

    GeM serialises IST wall-clock times with a 'Z' (UTC) suffix: a bid closing
    at 3 PM IST arrives as "...T15:00:00Z". Evidence: the site's own JavaScript
    renders these with UTC getters as local time, and the "ongoing" listing
    drops bids once their "...Z" end time passes in IST, five and a half hours
    before it would pass in UTC. So a trailing 'Z' means IST. An explicit
    offset (e.g. "+05:30"), should GeM ever send one, is honoured as-is.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"not a timestamp: {value!r}")
    text = value.strip()
    if text[-1] in "Zz":
        text = text[:-1]
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=IST)


def state_from_department(department: str | None) -> str | None:
    if not department:
        return None
    lowered = department.lower()
    for suffix, state in _STATE_SUFFIXES:
        if lowered == suffix or lowered.endswith(" " + suffix):
            return state
    return None


def document_url(source_id: str, bid_type_code: int | None, eval_type: int | None) -> str:
    """Mirror of the link logic in GeM's listing-page JavaScript."""
    if bid_type_code == 5:
        path = "showdirectradocumentPdf"
    elif bid_type_code == 2:
        path = "list-ra-schedules" if (eval_type or 0) > 0 else "showradocumentPdf"
    else:
        path = "showbidDocument"
    return f"{BASE_URL}/{path}/{source_id}"


def strip_dropped_fields(doc: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in doc.items() if key not in DROPPED_FIELDS}


def content_hash(raw: dict[str, Any]) -> str:
    """Stable fingerprint of a record: independent of key order, sensitive to any value."""
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"not an integer: {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value.strip())
    raise ValueError(f"not an integer: {value!r}")


def _to_int_or_none(value: Any) -> int | None:
    try:
        return _to_int(value)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _category_ids(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    unique = dict.fromkeys(part.strip() for part in str(value).split(","))
    return tuple(part for part in unique if part)


def _timestamp(raw: dict[str, Any], key: str, reason: str) -> datetime:
    try:
        return parse_gem_timestamp(unwrap(raw, key))
    except ValueError as exc:
        raise RecordRejected(reason, str(exc)) from exc


def normalize(doc: Any) -> Tender:
    """Validate and clean one raw listing record. Raises RecordRejected."""
    if not isinstance(doc, dict):
        raise RecordRejected("malformed_record", f"expected an object, got {type(doc).__name__}")
    raw = strip_dropped_fields(doc)

    source_id = unwrap(raw, "b_id")
    if source_id is None or str(source_id).strip() == "":
        raise RecordRejected("missing_source_id")
    source_id = str(source_id).strip()

    bid_number = clean_text(unwrap(raw, "b_bid_number"))
    if bid_number is None or not _BID_NUMBER_RE.fullmatch(bid_number):
        raise RecordRejected("invalid_bid_number", repr(bid_number))

    start_at = _timestamp(raw, "final_start_date_sort", "invalid_start_at")
    end_at = _timestamp(raw, "final_end_date_sort", "invalid_end_at")
    if end_at < start_at:
        raise RecordRejected("end_before_start", f"{start_at.isoformat()} > {end_at.isoformat()}")

    try:
        quantity = _to_int(unwrap(raw, "b_total_quantity"))
    except ValueError as exc:
        raise RecordRejected("invalid_quantity", str(exc)) from exc
    if quantity is not None and quantity < 0:
        raise RecordRejected("invalid_quantity", str(quantity))

    type_code = _to_int_or_none(unwrap(raw, "b_bid_type"))
    bid_type = "unknown" if type_code is None else BID_TYPES.get(type_code, "unknown")
    ministry = clean_text(unwrap(raw, "ba_official_details_minName"))
    department = clean_text(unwrap(raw, "ba_official_details_deptName"))
    items = clean_text(unwrap(raw, "b_category_name"))

    return Tender(
        source=SOURCE,
        source_id=source_id,
        bid_number=bid_number,
        bid_type=bid_type,
        parent_bid_number=clean_text(unwrap(raw, "b_bid_number_parent")),
        items=items,
        # b_category_name is cut at 100 characters, bd_category_name at 1024.
        items_detail=clean_text(unwrap(raw, "bd_category_name")) or items,
        quantity=quantity,
        ministry=ministry,
        department=department,
        state=None if ministry else state_from_department(department),
        start_at=start_at,
        end_at=end_at,
        is_high_value=_to_bool(unwrap(raw, "is_high_value")),
        is_single_packet=_to_bool(unwrap(raw, "ba_is_single_packet")),
        is_global_tender=_to_bool(unwrap(raw, "ba_is_global_tendering")),
        category_ids=_category_ids(unwrap(raw, "b_cat_id")),
        document_url=document_url(
            source_id, type_code, _to_int_or_none(unwrap(raw, "b_eval_type"))
        ),
        raw=raw,
        content_hash=content_hash(raw),
    )
