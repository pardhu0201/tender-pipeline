from tender_pipeline.normalize import normalize
from tender_pipeline.quality import QualityTracker


def tracked(listing_docs, rejections=0):
    tracker = QualityTracker()
    for doc in listing_docs:
        tracker.observe(normalize(doc))
    for i in range(rejections):
        tracker.observe_rejection("invalid_bid_number", f"bad-{i}")
    return tracker


def test_clean_run_has_no_issues(listing_docs):
    summary = tracked(listing_docs).summary(mode="full", source_total=12, complete=True)
    assert summary["issues"] == []
    assert summary["coverage"] == 1.0
    assert summary["accepted"] == 12
    # 3 of the 12 fixture records are state-government buyers with no ministry.
    assert summary["null_rate"]["ministry"] == 0.25


def test_high_reject_rate_is_an_issue(listing_docs):
    summary = tracked(listing_docs, rejections=1).summary(
        mode="incremental", source_total=None, complete=True
    )
    assert summary["reject_rate"] == round(1 / 13, 4)
    assert summary["rejected_by_reason"] == {"invalid_bid_number": 1}
    assert any("reject rate" in issue for issue in summary["issues"])


def test_low_coverage_only_counts_for_complete_full_crawls(listing_docs):
    tracker = tracked(listing_docs)
    assert tracker.summary(mode="full", source_total=100, complete=True)["issues"]
    assert not tracker.summary(mode="full", source_total=100, complete=False)["issues"]
    assert not tracker.summary(mode="incremental", source_total=100, complete=True)["issues"]


def test_duplicates_are_counted(listing_docs):
    tracker = tracked(listing_docs + listing_docs[:2])
    summary = tracker.summary(mode="full", source_total=12, complete=True)
    assert summary["duplicates_in_run"] == 2
    assert summary["unique_records"] == 12


def test_empty_full_crawl_is_an_issue():
    summary = QualityTracker().summary(mode="full", source_total=0, complete=True)
    assert summary["issues"] == ["GeM returned no records at all"]
