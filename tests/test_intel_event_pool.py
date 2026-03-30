from __future__ import annotations

import unittest

from services.intel.event_pool import merge_event_pool_entries


def _entry(*, event_key: str, signature_tokens: list[str], source: str, text: str) -> dict[str, object]:
    return {
        "topic": "world",
        "event_key": event_key,
        "signature_tokens": signature_tokens,
        "snapshot_hits": 1,
        "evidence_count": 1,
        "source_counts": {source: 1},
        "first_seen_at": 1,
        "last_seen_at": 1,
        "confirmation_count": 1 if source == "rss" else 0,
        "authority_score": 1.0 if source == "rss" else 0.0,
        "authority_keys": ["ap"] if source == "rss" else [],
        "representatives": {source: {"source": source, "text": text}},
    }


class IntelEventPoolTests(unittest.TestCase):
    def test_merge_keeps_distinct_events_when_only_partially_overlapping(self) -> None:
        entries = [
            _entry(
                event_key="event-a",
                signature_tokens=["war_escalation", "iran", "israel", "attack", "base"],
                source="x",
                text="attack on base",
            ),
            _entry(
                event_key="event-b",
                signature_tokens=["war_escalation", "iran", "israel", "attack", "embassy"],
                source="rss",
                text="attack on embassy",
            ),
        ]

        merged = merge_event_pool_entries(entries)

        self.assertEqual(len(merged), 2)

    def test_merge_allows_near_identical_signatures_with_same_core_terms(self) -> None:
        entries = [
            _entry(
                event_key="event-a",
                signature_tokens=["war_escalation", "iran", "israel", "attack", "base"],
                source="x",
                text="attack on base",
            ),
            _entry(
                event_key="event-b",
                signature_tokens=["war_escalation", "iran", "israel", "attack", "base", "missile"],
                source="rss",
                text="missile attack on base",
            ),
        ]

        merged = merge_event_pool_entries(entries)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["snapshot_hits"], 2)
        self.assertIn("missile", merged[0]["signature_tokens"])

    def test_merge_prefers_exact_event_key(self) -> None:
        entries = [
            _entry(
                event_key="event-a",
                signature_tokens=["war_escalation", "iran", "israel", "attack"],
                source="x",
                text="attack",
            ),
            _entry(
                event_key="event-a",
                signature_tokens=["war_escalation", "iran", "israel", "attack", "missile"],
                source="rss",
                text="missile attack",
            ),
        ]

        merged = merge_event_pool_entries(entries)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["event_key"], "event-a")


if __name__ == "__main__":
    unittest.main()
