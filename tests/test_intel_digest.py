from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest.mock import patch


monitor_stub = types.ModuleType("monitor")
monitor_stub.format_clock = lambda: "2026-03-30 08:00:00"
sys.modules["monitor"] = monitor_stub

digest = importlib.import_module("services.intel.digest")


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
    source_domain: str = "",
    external_domain: str = "",
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
        "source_domain": source_domain,
        "external_domain": external_domain,
        "topic_low_signal": False,
        "topic_promotional": False,
        "window_hits": window_hits,
        "window_span_hours": window_span_hours,
        "fetched_at": 1,
    }


class IntelDigestTests(unittest.TestCase):
    @patch.object(digest, "translate_text_to_chinese", autospec=True)
    def test_rebuild_digest_message_payload_translates_legacy_english_summary(
        self,
        mock_translate_text_to_chinese,
    ) -> None:
        mock_translate_text_to_chinese.side_effect = lambda value: {
            "Bitcoin jumps on ETF hopes": "比特币因 ETF 预期上涨",
            "Trump says sanctions may expand": "特朗普称制裁可能扩大",
        }.get(value, value)
        payload = {
            "digest_date": "2026-03-31",
            "config": {
                "daily_push_time": "08:00",
                "limits": {"hot": 2},
            },
            "sections": {
                "crypto": [
                    {
                        "source": "x",
                        "author": "user1",
                        "summary_text": "Bitcoin jumps on ETF hopes",
                        "original_text": "Bitcoin jumps on ETF hopes",
                        "url": "https://example.com/crypto",
                    }
                ],
                "world": [
                    {
                        "source": "rss",
                        "author": "user2",
                        "summary_text": "Trump says sanctions may expand",
                        "original_text": "Trump says sanctions may expand",
                        "url": "https://example.com/world",
                    }
                ],
                "persistent": [],
                "hot": [],
                "custom": [],
            },
            "message": "旧英文正文",
            "message_html": "old html",
        }

        rebuilt = digest.rebuild_digest_message_payload(payload)

        self.assertIn("比特币因 ETF 预期上涨", rebuilt["message"])
        self.assertIn("特朗普称制裁可能扩大", rebuilt["message"])
        self.assertNotIn("旧英文正文", rebuilt["message"])

    @patch.object(digest, "load_event_pool_since", autospec=True)
    def test_build_event_pool_candidate_pool_merges_stored_and_live_entries(
        self,
        mock_load_event_pool_since,
    ) -> None:
        stored_item = _item(
            item_id="stored-x",
            topic="crypto",
            source="x",
            text="BlackRock bitcoin ETF approval chatter continues on X.",
            cluster_id="crypto-shared",
            topic_event_matched=True,
            topic_event_type="regulation_etf",
            topic_event_label="ETF/监管",
            topic_event_score=3.4,
            topic_match_count=3,
            topic_news_match_count=1,
            topic_quality_score=14.0,
            score=220.0,
            event_confirmation_count=0,
            event_source_count=1,
            cluster_source_count=1,
            source_role="discovery",
        )
        stored_entries = digest.build_event_pool_entries(
            {"crypto": [stored_item], "world": [], "hot": [], "custom": []},
            captured_at=100,
            generated_at="2026-03-29 06:00:00",
            digest_date="2026-03-29",
        )
        mock_load_event_pool_since.return_value = stored_entries

        live_candidates = {
            "crypto": [
                _item(
                    item_id="live-rss",
                    topic="crypto",
                    source="rss",
                    text="Bloomberg confirms BlackRock spot bitcoin ETF approval filing progress.",
                    cluster_id="crypto-shared",
                    topic_event_matched=True,
                    topic_event_type="regulation_etf",
                    topic_event_label="ETF/监管",
                    topic_event_score=4.2,
                    topic_match_count=4,
                    topic_news_match_count=2,
                    topic_quality_score=16.0,
                    score=80.0,
                    event_confirmation_count=1,
                    event_source_count=2,
                    cluster_source_count=2,
                    signal_cross_count=1,
                    event_authority_score=2.3,
                    source_authority_score=2.3,
                    source_confirmed=True,
                    source_role="confirmation",
                    source_domain="bloomberg.com",
                )
            ],
            "world": [],
            "hot": [],
            "custom": [],
        }

        topics, meta = digest.build_event_pool_candidate_pool(
            live_candidates,
            since_ts=0,
            generated_at="2026-03-30 08:00:00",
            digest_date="2026-03-30",
        )

        crypto_items = topics["crypto"]
        self.assertEqual(len(crypto_items), 2)
        self.assertEqual(len({item["event_key"] for item in crypto_items}), 1)
        self.assertEqual({item["source"] for item in crypto_items}, {"x", "rss"})
        self.assertTrue(all(int(item["window_hits"]) == 2 for item in crypto_items))
        self.assertTrue(all(int(item["event_source_count"]) == 2 for item in crypto_items))
        self.assertTrue(all(int(item["event_confirmation_count"]) >= 1 for item in crypto_items))
        self.assertEqual(meta["event_pool_entry_count"], 2)
        self.assertEqual(meta["bootstrap_snapshot_count"], 0)

    @patch.object(digest, "load_snapshot_pool_since", autospec=True)
    @patch.object(digest, "load_event_pool_since", autospec=True)
    def test_build_event_pool_candidate_pool_bootstraps_from_snapshot_pool_when_empty(
        self,
        mock_load_event_pool_since,
        mock_load_snapshot_pool_since,
    ) -> None:
        mock_load_event_pool_since.return_value = []
        mock_load_snapshot_pool_since.return_value = [
            {
                "captured_at": 100,
                "generated_at": "2026-03-29 06:00:00",
                "digest_date": "2026-03-29",
                "topics": {
                    "crypto": [
                        _item(
                            item_id="snapshot-x",
                            topic="crypto",
                            source="x",
                            text="MicroStrategy buys more bitcoin.",
                            cluster_id="snapshot-crypto",
                            topic_event_matched=True,
                            topic_event_type="buying_treasury",
                            topic_event_label="增持/购入",
                            topic_event_score=3.2,
                            topic_match_count=3,
                            topic_news_match_count=1,
                            topic_quality_score=14.0,
                            score=200.0,
                            source_role="discovery",
                        )
                    ]
                },
            }
        ]

        topics, meta = digest.build_event_pool_candidate_pool(
            {"crypto": [], "world": [], "hot": [], "custom": []},
            since_ts=0,
        )

        self.assertEqual(meta["bootstrap_snapshot_count"], 1)
        self.assertGreater(meta["event_pool_entry_count"], 0)
        self.assertTrue(topics["crypto"])
        self.assertEqual(int(topics["crypto"][0]["window_hits"]), 1)

    @patch.object(digest, "apply_digest_summaries", autospec=True)
    @patch.object(digest, "load_sent_registry", autospec=True)
    def test_build_digest_payload_includes_selection_diagnostics_and_reason_text(
        self,
        mock_load_sent_registry,
        mock_apply_digest_summaries,
    ) -> None:
        mock_load_sent_registry.return_value = {"keys": {}, "updated_at": 0}
        mock_apply_digest_summaries.return_value = {
            "mode": "fallback_only",
            "model": "test-model",
            "counts": {"ai": 0, "fallback": 2},
        }
        cfg = {
            "timezone": "Asia/Shanghai",
            "daily_push_time": "08:00",
            "limits": {"crypto": 2, "world": 2, "hot": 1, "custom_user": 1},
            "summary": {"mode": "fallback_only", "model": "test-model"},
        }
        collected = {
            "crypto": [
                _item(
                    item_id="crypto-1",
                    topic="crypto",
                    source="rss",
                    text="BlackRock ETF approval confirmed by Bloomberg.",
                    cluster_id="crypto-cluster",
                    topic_event_matched=True,
                    topic_event_type="regulation_etf",
                    topic_event_label="ETF/监管",
                    topic_event_score=4.2,
                    topic_match_count=4,
                    topic_news_match_count=2,
                    topic_quality_score=16.0,
                    score=90.0,
                    event_confirmation_count=1,
                    event_source_count=2,
                    cluster_source_count=2,
                    signal_cross_count=1,
                    event_authority_score=2.3,
                    source_authority_score=2.3,
                    source_confirmed=True,
                    source_role="confirmation",
                )
            ],
            "world": [
                _item(
                    item_id="world-1",
                    topic="world",
                    source="x",
                    text="Iran and Israel ceasefire talks are progressing.",
                    cluster_id="world-cluster",
                    topic_event_matched=True,
                    topic_event_type="ceasefire_diplomacy",
                    topic_event_label="停火/外交",
                    topic_event_score=3.6,
                    topic_match_count=3,
                    topic_news_match_count=1,
                    topic_quality_score=14.5,
                    score=140.0,
                    event_confirmation_count=1,
                    event_source_count=2,
                    cluster_source_count=2,
                    signal_cross_count=1,
                    event_authority_score=1.5,
                    source_authority_score=0.7,
                    source_role="discovery",
                )
            ],
            "hot": [],
            "custom": [],
        }

        payload = digest.build_digest_payload(
            final=False,
            respect_sent=False,
            cfg=cfg,
            collected=collected,
        )

        self.assertTrue(payload["ok"])
        self.assertIn("selection_diagnostics", payload["build_stats"])
        self.assertEqual(payload["build_stats"]["selection_diagnostics"]["overall"]["total"], 2)
        self.assertEqual(payload["build_stats"]["selection_diagnostics"]["lanes"]["crypto"]["confirmed"], 1)
        self.assertEqual(payload["build_stats"]["selection_diagnostics"]["lanes"]["world"]["confirmed"], 1)
        self.assertTrue(payload["sections"]["crypto"][0]["selection_reason_text"])
        self.assertIn("确认源", payload["sections"]["crypto"][0]["selection_reason_text"])
        self.assertTrue(payload["message_html"])

    @patch.object(digest, "apply_digest_summaries", autospec=True)
    @patch.object(digest, "load_sent_registry", autospec=True)
    def test_build_digest_payload_keeps_window_meta_and_persistent_event_from_aggregate_pool(
        self,
        mock_load_sent_registry,
        mock_apply_digest_summaries,
    ) -> None:
        mock_load_sent_registry.return_value = {"keys": {}, "updated_at": 0}
        mock_apply_digest_summaries.return_value = {
            "mode": "fallback_only",
            "model": "test-model",
            "counts": {"ai": 0, "fallback": 2},
        }
        cfg = {
            "timezone": "Asia/Shanghai",
            "daily_push_time": "08:00",
            "limits": {"crypto": 1, "world": 0, "hot": 0, "custom_user": 0},
            "summary": {"mode": "fallback_only", "model": "test-model"},
        }
        collected = {
            "crypto": [
                _item(
                    item_id="crypto-main",
                    topic="crypto",
                    source="x",
                    text="Bitcoin ETF approval dominates X today.",
                    cluster_id="crypto-main",
                    topic_event_matched=True,
                    topic_event_type="regulation_etf",
                    topic_event_label="ETF/监管",
                    topic_event_score=4.0,
                    topic_match_count=4,
                    topic_news_match_count=1,
                    topic_quality_score=16.5,
                    score=900.0,
                    event_confirmation_count=1,
                    event_source_count=2,
                    cluster_source_count=2,
                    signal_cross_count=1,
                    window_hits=1,
                    source_role="discovery",
                ),
                _item(
                    item_id="crypto-persistent",
                    topic="crypto",
                    source="rss",
                    text="Stablecoin regulation event keeps developing across multiple collection rounds.",
                    cluster_id="crypto-persistent",
                    topic_event_matched=True,
                    topic_event_type="regulation_policy",
                    topic_event_label="监管/政策",
                    topic_event_score=3.6,
                    topic_match_count=3,
                    topic_news_match_count=2,
                    topic_quality_score=13.5,
                    score=120.0,
                    event_confirmation_count=1,
                    event_source_count=2,
                    cluster_source_count=2,
                    signal_cross_count=1,
                    window_hits=3,
                    window_span_hours=4.0,
                    source_confirmed=True,
                    source_role="confirmation",
                    event_authority_score=2.2,
                    source_authority_score=2.2,
                ),
            ],
            "world": [],
            "hot": [],
            "custom": [],
        }

        payload = digest.build_digest_payload(
            final=False,
            respect_sent=False,
            cfg=cfg,
            collected=collected,
            window_meta={"event_pool_entry_count": 6, "event_count": 2, "since_ts": 123},
        )

        self.assertEqual(payload["counts"]["crypto"], 1)
        self.assertEqual(payload["counts"]["persistent"], 1)
        self.assertEqual(payload["build_stats"]["window"]["event_pool_entry_count"], 6)
        self.assertEqual(payload["sections"]["persistent"][0]["id"], "crypto-persistent")
        self.assertIn("持续 3 轮", payload["sections"]["persistent"][0]["selection_reason_text"])

    @patch.object(digest, "save_latest_sent_digest", autospec=True)
    @patch.object(digest, "save_sent_registry", autospec=True)
    @patch.object(digest, "load_sent_registry", autospec=True)
    def test_finalize_digest_payload_marks_final_and_saves_latest_sent(
        self,
        mock_load_sent_registry,
        mock_save_sent_registry,
        mock_save_latest_sent_digest,
    ) -> None:
        mock_load_sent_registry.return_value = {
            "keys": {"existing-key": 123},
            "updated_at": 123,
        }
        payload = {
            "sections": {
                "crypto": [_item(item_id="fresh", topic="crypto", source="x", text="BTC ETF live", cluster_id="c1")],
            },
        }

        finalized = digest.finalize_digest_payload(payload)

        saved_registry = mock_save_sent_registry.call_args.args[0]
        self.assertIn("existing-key", saved_registry["keys"])
        self.assertTrue(finalized["final"])
        mock_save_latest_sent_digest.assert_called_once()


if __name__ == "__main__":
    unittest.main()
