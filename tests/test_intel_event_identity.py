from __future__ import annotations

import unittest

from services.intel.event_identity import build_group_event_identity, build_item_event_identity


class IntelEventIdentityTests(unittest.TestCase):
    def test_item_identity_normalizes_entity_and_action_aliases(self) -> None:
        item = {
            "topic": "crypto",
            "text": "BlackRock's BTC ETF was approved after SEC filing.",
            "topic_event_type": "regulation_etf",
            "topic_event_entity_hits": ["btc", "blackrock", "sec"],
            "topic_event_action_hits": ["approved", "filing"],
            "topic_event_evidence_hits": ["spot etf"],
        }

        identity = build_item_event_identity(item)

        self.assertEqual(identity["event_identity_terms"][:4], ["regulation_etf", "bitcoin", "blackrock", "sec"])
        self.assertIn("approval", identity["event_identity_terms"])
        self.assertTrue(identity["event_identity_key"])

    def test_group_identity_is_stable_across_alias_variants(self) -> None:
        item_a = {
            "topic": "world",
            "text": "Israel launched airstrikes on Iran after missile warning.",
            "topic_event_type": "war_escalation",
            "topic_event_entity_hits": ["israel", "iran"],
            "topic_event_action_hits": ["airstrikes", "missile"],
            "topic_event_evidence_hits": ["military"],
        }
        item_b = {
            "topic": "world",
            "text": "Iran says Israeli missile attack escalates the conflict.",
            "topic_event_type": "war_escalation",
            "topic_event_entity_hits": ["iran", "israel"],
            "topic_event_action_hits": ["attack", "missiles"],
            "topic_event_evidence_hits": ["operation"],
        }

        key_one, terms_one = build_group_event_identity("world", [item_a, item_b])
        key_two, terms_two = build_group_event_identity("world", [item_b, item_a])

        self.assertEqual(key_one, key_two)
        self.assertEqual(terms_one, terms_two)
        self.assertIn("war_escalation", terms_one)
        self.assertIn("iran", terms_one)
        self.assertIn("israel", terms_one)

    def test_group_identity_distinguishes_different_actions(self) -> None:
        ceasefire_item = {
            "topic": "world",
            "text": "Iran and Israel enter ceasefire talks through mediators.",
            "topic_event_type": "ceasefire_diplomacy",
            "topic_event_entity_hits": ["iran", "israel"],
            "topic_event_action_hits": ["ceasefire", "talks"],
            "topic_event_evidence_hits": ["agreement"],
        }
        strike_item = {
            "topic": "world",
            "text": "Iran and Israel exchange missile attacks overnight.",
            "topic_event_type": "war_escalation",
            "topic_event_entity_hits": ["iran", "israel"],
            "topic_event_action_hits": ["attack", "missile"],
            "topic_event_evidence_hits": ["military"],
        }

        ceasefire_key, _ = build_group_event_identity("world", [ceasefire_item])
        strike_key, _ = build_group_event_identity("world", [strike_item])

        self.assertNotEqual(ceasefire_key, strike_key)

    def test_item_identity_filters_event_type_and_noise_context_terms(self) -> None:
        item = {
            "topic": "world",
            "text": "Iran and Israel diplomacy talks hit largest stage as mediators push ceasefire.",
            "topic_event_type": "ceasefire_diplomacy",
            "topic_event_entity_hits": ["iran", "israel"],
            "topic_event_action_hits": ["talks"],
            "topic_event_evidence_hits": ["talks"],
        }

        identity = build_item_event_identity(item)

        self.assertNotIn("diplomacy", identity["event_identity_terms"])
        self.assertNotIn("hit", identity["event_identity_terms"])
        self.assertNotIn("largest", identity["event_identity_terms"])


if __name__ == "__main__":
    unittest.main()
