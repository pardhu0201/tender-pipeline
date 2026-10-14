"""Per-run data-quality accounting, and the gates that mark a run as degraded.

A degraded run still commits its data; it raises an alert so a human looks
at it. Hard failures (network down, site changed shape) fail the run instead.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from tender_pipeline.normalize import Tender

MAX_REJECT_RATE = 0.05  # more than 5% of records failing validation
MIN_FULL_COVERAGE = 0.95  # a complete full crawl must see 95% of what the source reports
TRACKED_FIELDS = ("items", "quantity", "ministry", "department")


@dataclass
class QualityTracker:
    accepted: int = 0
    rejected_by_reason: Counter[str] = field(default_factory=Counter)
    missing: Counter[str] = field(default_factory=Counter)
    unknown_bid_types: int = 0
    buyer_unidentified: int = 0  # neither a ministry nor a recognisable state
    duplicates: int = 0
    _seen_ids: set[str] = field(default_factory=set)

    def observe(self, tender: Tender) -> None:
        self.accepted += 1
        self._see(tender.source_id)
        for name in TRACKED_FIELDS:
            if getattr(tender, name) is None:
                self.missing[name] += 1
        if tender.bid_type == "unknown":
            self.unknown_bid_types += 1
        if tender.ministry is None and tender.state is None:
            self.buyer_unidentified += 1

    def observe_rejection(self, reason: str, source_id: str | None = None) -> None:
        self.rejected_by_reason[reason] += 1
        if source_id is not None:
            self._see(source_id)

    def _see(self, source_id: str) -> None:
        # Offset pagination over a live listing can show a record twice when
        # new bids push older ones onto the next page. Harmless (the upsert is
        # idempotent) but worth counting.
        if source_id in self._seen_ids:
            self.duplicates += 1
        else:
            self._seen_ids.add(source_id)

    def summary(self, *, mode: str, source_total: int | None, complete: bool) -> dict[str, Any]:
        rejected = sum(self.rejected_by_reason.values())
        seen = self.accepted + rejected
        reject_rate = rejected / seen if seen else 0.0

        coverage = None
        if mode == "full" and complete and source_total:
            coverage = round(min(1.0, len(self._seen_ids) / source_total), 4)

        issues = []
        if reject_rate > MAX_REJECT_RATE:
            issues.append(f"reject rate {reject_rate:.1%} is above {MAX_REJECT_RATE:.0%}")
        if coverage is not None and coverage < MIN_FULL_COVERAGE:
            issues.append(
                f"full crawl saw {coverage:.1%} of the {source_total} records GeM reported "
                f"(minimum {MIN_FULL_COVERAGE:.0%})"
            )
        if mode == "full" and complete and seen == 0:
            issues.append("GeM returned no records at all")

        return {
            "accepted": self.accepted,
            "rejected": rejected,
            "reject_rate": round(reject_rate, 4),
            "rejected_by_reason": dict(self.rejected_by_reason),
            "null_rate": {
                name: round(self.missing[name] / self.accepted, 4) if self.accepted else 0.0
                for name in TRACKED_FIELDS
            },
            "unknown_bid_types": self.unknown_bid_types,
            "buyer_unidentified": self.buyer_unidentified,
            "unique_records": len(self._seen_ids),
            "duplicates_in_run": self.duplicates,
            "coverage": coverage,
            "issues": issues,
        }
