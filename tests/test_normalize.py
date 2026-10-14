from datetime import UTC, datetime, timedelta

import pytest

from tender_pipeline.normalize import (
    IST,
    RecordRejected,
    clean_text,
    content_hash,
    normalize,
    parse_gem_timestamp,
    state_from_department,
)

# -- timestamps --------------------------------------------------------------


def test_gem_z_timestamps_are_ist_wall_clock():
    # GeM's "Z" is not UTC: "20:00Z" is 8 PM in India, i.e. 14:30 UTC.
    parsed = parse_gem_timestamp("2026-10-13T20:00:00Z")
    assert parsed.utcoffset() == timedelta(hours=5, minutes=30)
    assert parsed.astimezone(UTC) == datetime(2026, 10, 13, 14, 30, tzinfo=UTC)


def test_explicit_offsets_are_honoured():
    parsed = parse_gem_timestamp("2026-10-13T20:00:00+00:00")
    assert parsed.astimezone(UTC) == datetime(2026, 10, 13, 20, 0, tzinfo=UTC)


def test_naive_timestamps_are_treated_as_ist():
    assert parse_gem_timestamp("2026-10-13T20:00:00").tzinfo == IST


@pytest.mark.parametrize("value", [None, "", "   ", "yesterday", 1760000000, ["2026-10-13"]])
def test_unparseable_timestamps_raise(value):
    with pytest.raises(ValueError):
        parse_gem_timestamp(value)


# -- text cleanup -------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("  Indian   Railways ", "Indian Railways"),
        ("NA", None),
        ("n/a", None),
        ("-", None),
        ("", None),
        (None, None),
        ("NATIONAL HIGHWAYS", "NATIONAL HIGHWAYS"),  # only whole-value placeholders
    ],
)
def test_clean_text(value, expected):
    assert clean_text(value) == expected


@pytest.mark.parametrize(
    ("department", "state"),
    [
        ("Urban Development and Housing Department Jharkhand", "Jharkhand"),
        ("Energy Department Uttar Pradesh", "Uttar Pradesh"),
        ("Energy Department Andhra Pradesh", "Andhra Pradesh"),
        ("Revenue Department Orissa", "Odisha"),  # old name
        ("Home Department Jammu & Kashmir", "Jammu and Kashmir"),
        (
            "Tourism Department Dadra and Nagar Haveli and Daman and Diu",
            "Dadra and Nagar Haveli and Daman and Diu",
        ),
        ("Goa", "Goa"),
        ("Department of Panchayati Raj", None),  # no state named
        ("Assamese Studies Board", None),  # a prefix, not a suffix
        (None, None),
    ],
)
def test_state_from_department(department, state):
    assert state_from_department(department) == state


# -- whole records (real GeM data) ----------------------------------------------


def test_every_fixture_record_normalizes(listing_docs):
    tenders = [normalize(doc) for doc in listing_docs]
    assert len(tenders) == len(listing_docs) == 12
    assert len({t.source_id for t in tenders}) == 12
    assert all(t.source == "gem" and t.end_at >= t.start_at for t in tenders)


def test_central_buyer(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/B/8041403"))
    assert tender.ministry == "Ministry of Education"
    assert tender.department == "Department of Higher Education"
    assert tender.state is None
    assert tender.bid_type == "bid"
    assert tender.quantity == 1
    assert tender.document_url == "https://bidplus.gem.gov.in/showbidDocument/9898616"


def test_state_buyer_gets_state_from_department(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/B/8110219"))
    assert tender.ministry is None
    assert tender.state == "Jharkhand"


def test_placeholder_department_becomes_null(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/B/8094041"))
    assert tender.ministry == "Ministry of AYUSH"
    assert tender.department is None


def test_whitespace_is_collapsed_everywhere(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/B/8052275"))
    for value in (tender.items, tender.items_detail, tender.ministry, tender.department):
        assert value == " ".join(value.split())


def test_reverse_auction(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/R/750456"))
    assert tender.bid_type == "reverse_auction"
    assert tender.parent_bid_number == "GEM/2026/B/8099767"
    assert tender.document_url == "https://bidplus.gem.gov.in/showradocumentPdf/10017598"


def test_evaluated_reverse_auction_links_to_its_schedule(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/R/752138"))
    assert tender.document_url == "https://bidplus.gem.gov.in/list-ra-schedules/10030598"


def test_direct_reverse_auction(fixture_doc):
    doc = fixture_doc("GEM/2026/R/750456")
    doc["b_bid_type"] = [5]
    tender = normalize(doc)
    assert tender.bid_type == "direct_reverse_auction"
    assert tender.document_url.endswith("/showdirectradocumentPdf/10017598")


def test_unknown_bid_type_is_kept_but_labelled(fixture_doc):
    doc = fixture_doc("GEM/2026/B/8041403")
    doc["b_bid_type"] = [9]
    assert normalize(doc).bid_type == "unknown"


def test_missing_detail_falls_back_to_items(fixture_doc):
    tender = normalize(fixture_doc("GEM/2026/R/752132"))
    assert tender.items and tender.items_detail == tender.items


def test_flags_and_categories(fixture_doc):
    assert normalize(fixture_doc("GEM/2026/B/8114345")).is_high_value is True
    assert normalize(fixture_doc("GEM/2026/B/8076526")).is_single_packet is True
    assert normalize(fixture_doc("GEM/2026/B/8114297")).category_ids == (
        "home_el05420327_lamp_lamp_se47014418",
        "home_el63626345_lamp_lamp_whit",
    )


def test_creator_account_id_is_not_stored(listing_docs):
    assert all("b.b_created_by" in doc for doc in listing_docs)
    assert all("b.b_created_by" not in normalize(doc).raw for doc in listing_docs)


def test_content_hash_ignores_key_order_but_not_values(fixture_doc):
    doc = fixture_doc("GEM/2026/B/8041403")
    reordered = dict(reversed(list(doc.items())))
    assert normalize(doc).content_hash == normalize(reordered).content_hash

    doc["final_end_date_sort"] = ["2026-10-20T20:00:00Z"]  # e.g. a corrigendum extends the deadline
    assert normalize(doc).content_hash != normalize(reordered).content_hash
    assert content_hash({"a": 1, "b": [2]}) == content_hash({"b": [2], "a": 1})


# -- rejections -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("b_id", None, "missing_source_id"),
        ("b_id", [""], "missing_source_id"),
        ("b_bid_number", ["GEM/2026/X/123"], "invalid_bid_number"),
        ("b_bid_number", [], "invalid_bid_number"),
        ("final_start_date_sort", ["soon"], "invalid_start_at"),
        ("final_end_date_sort", None, "invalid_end_at"),
        ("final_end_date_sort", ["2026-09-01T10:00:00Z"], "end_before_start"),
        ("b_total_quantity", [-5], "invalid_quantity"),
        ("b_total_quantity", ["lots"], "invalid_quantity"),
    ],
)
def test_invalid_records_are_rejected_with_a_reason(fixture_doc, field, value, reason):
    doc = fixture_doc("GEM/2026/B/8041403")
    if value is None:
        doc.pop(field)
    else:
        doc[field] = value
    with pytest.raises(RecordRejected) as excinfo:
        normalize(doc)
    assert excinfo.value.reason == reason


def test_non_object_records_are_rejected():
    with pytest.raises(RecordRejected) as excinfo:
        normalize(["not", "a", "record"])
    assert excinfo.value.reason == "malformed_record"


def test_missing_quantity_is_allowed(fixture_doc):
    doc = fixture_doc("GEM/2026/B/8041403")
    doc.pop("b_total_quantity")
    assert normalize(doc).quantity is None


@pytest.mark.parametrize(
    ("quantity", "flag", "expected_quantity", "expected_flag"),
    [
        (["42"], ["true"], 42, True),  # numbers and flags as strings
        ([7.0], [1.0], 7, True),  # floats
        ([3], [0], 3, False),
        ([3], ["no"], 3, False),
        ([3], [None], 3, False),
    ],
)
def test_numbers_and_flags_in_other_encodings(
    fixture_doc, quantity, flag, expected_quantity, expected_flag
):
    doc = fixture_doc("GEM/2026/B/8041403")
    doc["b_total_quantity"], doc["is_high_value"] = quantity, flag
    tender = normalize(doc)
    assert (tender.quantity, tender.is_high_value) == (expected_quantity, expected_flag)


@pytest.mark.parametrize("quantity", [[True], [2.5], [{"n": 1}]])
def test_quantities_that_are_not_integers_are_rejected(fixture_doc, quantity):
    doc = fixture_doc("GEM/2026/B/8041403")
    doc["b_total_quantity"] = quantity
    with pytest.raises(RecordRejected) as excinfo:
        normalize(doc)
    assert excinfo.value.reason == "invalid_quantity"
