from __future__ import annotations

import unittest

from services.intel.ranking import (
    select_items_with_x_priority,
    select_persistent_items,
    select_world_items,
)


def _item(
    *,
    item_id: str,
    topic: str,
    source: str,
    text: str,
    cluster_id: str,
    topic_event_matched: bool = False,
    topic_event_type: str = "",
    topic_event_label: str = "",
    topic_event_score: float = 0.0,
    topic_match_count: int = 2,
    topic_news_match_count: int = 1,
    topic_quality_score: float = 12.0,
    score: float = 80.0,
    signal_cross_count: int = 0,
    cluster_source_count: int = 1,
    event_confirmation_count: int = 0,
    event_source_count: int = 1,
    event_authority_score: float = 0.7,
    source_authority_score: float = 0.7,
    source_confirmed: bool = False,
    source_role: str = "discovery",
    topic_low_signal: bool = False,
    topic_promotional: bool = False,
    window_hits: int = 1,
    window_span_hours: float = 0.0,
) -> dict[str, object]:
    return {
        "id": item_id,
        "topic": topic,
        "source": source,
        "text": text,
        "url": f"https://example.com/{item_id}",
        "author": f"user_{item_id}",
        "cluster_id": cluster_id,
        "topic_event_matched": topic_event_matched,
        "topic_event_type": topic_event_type,
        "topic_event_label": topic_event_label,
        "topic_event_score": topic_event_score,
        "topic_match_count": topic_match_count,
        "topic_news_match_count": topic_news_match_count,
        "topic_quality_score": topic_quality_score,
        "score": score,
        "signal_cross_count": signal_cross_count,
        "cluster_source_count": cluster_source_count,
        "event_confirmation_count": event_confirmation_count,
        "event_source_count": event_source_count,
        "event_authority_score": event_authority_score,
        "source_authority_score": source_authority_score,
        "source_confirmed": source_confirmed,
        "source_role": source_role,
        "topic_low_signal": topic_low_signal,
        "topic_promotional": topic_promotional,
        "window_hits": window_hits,
        "window_span_hours": window_span_hours,
    }


class IntelRankingTests(unittest.TestCase):
    def test_crypto_selection_prefers_confirmed_event_over_hype_x_post(self) -> None:
        confirmed_event = _item(
            item_id="confirmed",
            topic="crypto",
            source="rss",
            text="BlackRock spot bitcoin ETF approval confirmed.",
            cluster_id="cluster-confirmed",
            topic_event_matched=True,
            topic_event_type="regulation_etf",
            topic_event_label="ETF/监管",
            topic_event_score=4.0,
            topic_match_count=4,
            topic_news_match_count=2,
            topic_quality_score=16.0,
            score=40.0,
            event_confirmation_count=1,
            event_source_count=2,
            cluster_source_count=2,
            event_authority_score=2.2,
            source_authority_score=2.2,
            source_confirmed=True,
            source_role="confirmation",
        )
        hype_x = _item(
            item_id="hype",
            topic="crypto",
            source="x",
            text="BTC to the moon 100x now.",
            cluster_id="cluster-hype",
            topic_event_matched=False,
            topic_match_count=3,
            topic_news_match_count=0,
            topic_quality_score=13.5,
            score=5000.0,
            event_authority_score=0.7,
            source_authority_score=0.7,
            source_role="discovery",
        )

        selected = select_items_with_x_priority([hype_x, confirmed_event], 1)

        self.assertEqual([item["id"] for item in selected], ["confirmed"])

    def test_world_selection_prefers_confirmed_event_over_unconfirmed_x(self) -> None:
        noisy_x = _item(
            item_id="x-only",
            topic="world",
            source="x",
            text="Breaking conflict update.",
            cluster_id="cluster-x",
            topic_event_matched=True,
            topic_event_type="war_escalation",
            topic_event_label="战争升级",
            topic_event_score=3.2,
            topic_match_count=3,
            topic_news_match_count=0,
            topic_quality_score=14.0,
            score=1800.0,
            event_confirmation_count=0,
            cluster_source_count=1,
            event_authority_score=0.7,
            source_authority_score=0.7,
            source_role="discovery",
        )
        confirmed_world = _item(
            item_id="confirmed-world",
            topic="world",
            source="rss",
            text="Reuters confirms ceasefire talks between Iran and Israel.",
            cluster_id="cluster-rss",
            topic_event_matched=True,
            topic_event_type="ceasefire_diplomacy",
            topic_event_label="停火/外交",
            topic_event_score=4.1,
            topic_match_count=4,
            topic_news_match_count=2,
            topic_quality_score=16.5,
            score=60.0,
            event_confirmation_count=1,
            event_source_count=2,
            cluster_source_count=2,
            signal_cross_count=1,
            event_authority_score=2.4,
            source_authority_score=2.4,
            source_confirmed=True,
            source_role="confirmation",
        )

        selected = select_world_items([noisy_x, confirmed_world], 1)

        self.assertEqual([item["id"] for item in selected], ["confirmed-world"])

    def test_persistent_selection_requires_event_or_confirmation_for_core_topics(self) -> None:
        unsupported_hype = _item(
            item_id="unsupported",
            topic="crypto",
            source="x",
            text="BTC still hot again.",
            cluster_id="cluster-unsupported",
            topic_event_matched=False,
            topic_match_count=3,
            topic_news_match_count=0,
            topic_quality_score=14.0,
            score=300.0,
            window_hits=3,
            window_span_hours=5.0,
            event_confirmation_count=0,
            event_source_count=1,
            source_role="discovery",
        )
        qualified_event = _item(
            item_id="qualified",
            topic="crypto",
            source="rss",
            text="ETF approval keeps developing across sessions.",
            cluster_id="cluster-qualified",
            topic_event_matched=True,
            topic_event_type="regulation_etf",
            topic_event_label="ETF/监管",
            topic_event_score=4.0,
            topic_match_count=4,
            topic_news_match_count=2,
            topic_quality_score=16.0,
            score=120.0,
            window_hits=3,
            window_span_hours=5.0,
            event_confirmation_count=1,
            event_source_count=2,
            signal_cross_count=1,
            cluster_source_count=2,
            source_confirmed=True,
            source_role="confirmation",
            event_authority_score=2.2,
            source_authority_score=2.2,
        )

        selected = select_persistent_items([unsupported_hype, qualified_event], 3)

        self.assertEqual([item["id"] for item in selected], ["qualified"])

    def test_selection_does_not_repeat_same_cluster_to_fill_limit(self) -> None:
        x_item = _item(
            item_id="cluster-x",
            topic="crypto",
            source="x",
            text="Bitcoin ETF chatter on X.",
            cluster_id="shared-cluster",
            topic_event_matched=True,
            topic_event_type="regulation_etf",
            topic_event_label="ETF/监管",
            topic_event_score=3.4,
            topic_quality_score=14.0,
            topic_match_count=3,
            topic_news_match_count=1,
            score=120.0,
            event_confirmation_count=1,
            event_source_count=2,
            signal_cross_count=1,
            cluster_source_count=2,
        )
        rss_item = _item(
            item_id="cluster-rss",
            topic="crypto",
            source="rss",
            text="ETF confirmation from RSS.",
            cluster_id="shared-cluster",
            topic_event_matched=True,
            topic_event_type="regulation_etf",
            topic_event_label="ETF/监管",
            topic_event_score=4.0,
            topic_quality_score=15.0,
            topic_match_count=4,
            topic_news_match_count=2,
            score=60.0,
            event_confirmation_count=1,
            event_source_count=2,
            signal_cross_count=1,
            cluster_source_count=2,
            source_confirmed=True,
            source_role="confirmation",
            event_authority_score=2.1,
            source_authority_score=2.1,
        )

        selected = select_items_with_x_priority([x_item, rss_item], 2)

        self.assertEqual(len(selected), 1)
        self.assertEqual(len({item["cluster_id"] for item in selected}), 1)


if __name__ == "__main__":
    unittest.main()
