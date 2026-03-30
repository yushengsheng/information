from __future__ import annotations

import importlib
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import patch


monitor_stub = types.ModuleType("monitor")
monitor_stub.format_clock = lambda: "2026-03-30 08:00:00"
sys.modules["monitor"] = monitor_stub

delivery = importlib.import_module("services.intel.delivery")


def _cfg_with_x() -> dict[str, object]:
    return {
        "timezone": "Asia/Shanghai",
        "daily_push_time": "08:00",
        "daily_enabled": True,
        "telegram": {
            "enabled": True,
            "chat_id": "12345",
            "chat_title": "Test Chat",
            "chat_type": "private",
            "bot_username": "test_bot",
        },
        "fixed": {
            "crypto": {"x_queries": ["bitcoin etf"]},
            "world": {"x_queries": []},
            "hot": {"x_queries": []},
        },
        "custom": {"x_users": []},
    }


def _payload_with_raw_x_count(raw_x: int, *, message: str = "日报正文") -> dict[str, object]:
    return {
        "ok": True,
        "message": message,
        "message_html": "<b>日报正文</b>",
        "build_stats": {
            "source_counts": {
                "crypto": {"x": raw_x},
                "world": {},
                "persistent": {},
                "hot": {},
                "custom": {},
            }
        },
    }


class IntelDeliveryTests(unittest.TestCase):
    @patch.object(delivery.time, "sleep", autospec=True)
    @patch.object(delivery, "build_digest_payload", autospec=True)
    @patch.object(delivery, "build_event_pool_candidate_pool", autospec=True)
    @patch.object(delivery, "collect_digest_candidates", autospec=True)
    def test_build_event_pool_payload_retries_when_x_is_missing_then_recovers(
        self,
        mock_collect_digest_candidates,
        mock_build_event_pool_candidate_pool,
        mock_build_digest_payload,
        mock_sleep,
    ) -> None:
        cfg = _cfg_with_x()
        live_candidates = {"crypto": [], "world": [], "hot": [], "custom": []}
        aggregate_candidates = {"crypto": [], "world": [], "hot": [], "custom": []}
        aggregate_meta = {"event_pool_entry_count": 4, "bootstrap_snapshot_count": 0}
        mock_collect_digest_candidates.side_effect = [live_candidates, live_candidates]
        mock_build_event_pool_candidate_pool.side_effect = [
            (aggregate_candidates, aggregate_meta),
            (aggregate_candidates, aggregate_meta),
        ]
        mock_build_digest_payload.side_effect = [
            _payload_with_raw_x_count(0),
            _payload_with_raw_x_count(2),
        ]

        payload, meta = delivery._build_event_pool_payload(
            cfg,
            previous_sent_cutoff_ts=123,
            respect_sent=True,
        )

        self.assertEqual(payload["build_stats"]["source_counts"]["crypto"]["x"], 2)
        self.assertTrue(meta["x_retry_triggered"])
        self.assertTrue(meta["x_retry_recovered"])
        self.assertEqual(len(meta["x_retry_attempts"]), 2)
        self.assertEqual(meta["x_retry_attempts"][0]["raw_x_total"], 0)
        self.assertEqual(meta["x_retry_attempts"][1]["raw_x_total"], 2)
        self.assertEqual(meta["aggregate_window_since_ts"], 123)
        mock_sleep.assert_called_once_with(20.0)
        self.assertEqual(mock_collect_digest_candidates.call_count, 2)
        self.assertEqual(mock_build_event_pool_candidate_pool.call_count, 2)
        self.assertEqual(mock_build_digest_payload.call_count, 2)

    @patch.object(delivery.time, "sleep", autospec=True)
    @patch.object(delivery, "build_digest_payload", autospec=True)
    @patch.object(delivery, "build_event_pool_candidate_pool", autospec=True)
    @patch.object(delivery, "collect_digest_candidates", autospec=True)
    def test_build_event_pool_payload_skips_retry_when_no_x_topics_configured(
        self,
        mock_collect_digest_candidates,
        mock_build_event_pool_candidate_pool,
        mock_build_digest_payload,
        mock_sleep,
    ) -> None:
        cfg = {
            "timezone": "Asia/Shanghai",
            "fixed": {"crypto": {"x_queries": []}, "world": {"x_queries": []}, "hot": {"x_queries": []}},
            "custom": {"x_users": []},
        }
        live_candidates = {"crypto": [], "world": [], "hot": [], "custom": []}
        aggregate_candidates = {"crypto": [], "world": [], "hot": [], "custom": []}
        aggregate_meta = {"event_pool_entry_count": 1, "bootstrap_snapshot_count": 0}
        mock_collect_digest_candidates.return_value = live_candidates
        mock_build_event_pool_candidate_pool.return_value = (aggregate_candidates, aggregate_meta)
        mock_build_digest_payload.return_value = _payload_with_raw_x_count(0)

        _, meta = delivery._build_event_pool_payload(
            cfg,
            previous_sent_cutoff_ts=None,
            respect_sent=False,
        )

        self.assertFalse(meta["x_retry_triggered"])
        self.assertEqual(len(meta["x_retry_attempts"]), 1)
        mock_sleep.assert_not_called()
        mock_collect_digest_candidates.assert_called_once()

    @patch.object(delivery, "_save_daily_state", autospec=True)
    @patch.object(delivery, "finalize_digest_payload", autospec=True)
    @patch.object(delivery, "send_telegram_message", autospec=True)
    @patch.object(delivery, "get_digest_message_html", autospec=True)
    @patch.object(delivery, "_build_event_pool_payload", autospec=True)
    @patch.object(delivery, "_load_daily_state", autospec=True)
    @patch.object(delivery, "_auto_bind_chat", autospec=True)
    @patch.object(delivery, "_refresh_bot_username", autospec=True)
    @patch.object(delivery, "_load_runtime_context", autospec=True)
    def test_run_daily_delivery_sends_html_and_updates_state(
        self,
        mock_load_runtime_context,
        mock_refresh_bot_username,
        mock_auto_bind_chat,
        mock_load_daily_state,
        mock_build_event_pool_payload,
        mock_get_digest_message_html,
        mock_send_telegram_message,
        mock_finalize_digest_payload,
        mock_save_daily_state,
    ) -> None:
        cfg = _cfg_with_x()
        telegram = dict(cfg["telegram"])
        payload = {"message": "日报正文", "message_html": "<b>日报正文</b>"}
        mock_load_runtime_context.return_value = (cfg, telegram, "token-123")
        mock_refresh_bot_username.return_value = cfg
        mock_auto_bind_chat.return_value = cfg
        mock_load_daily_state.return_value = ({}, {"last_sent_date": "", "last_sent_at": "", "last_attempt_at": "", "last_error": ""})
        mock_build_event_pool_payload.return_value = (payload, {"x_retry_triggered": False})
        mock_get_digest_message_html.return_value = "<b>日报正文</b>"
        mock_send_telegram_message.return_value = [{"message_id": 101}]
        mock_save_daily_state.side_effect = [
            {
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
                "last_error": "",
                "pending_delivery": {"state": "sending"},
            },
            {
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
                "last_error": "",
                "pending_delivery": {"state": "sent_not_committed"},
            },
            {
                "last_sent_date": "2026-03-30",
                "last_sent_at": "2026-03-30T08:00:00+08:00",
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
                "last_error": "",
            },
        ]

        result = delivery.run_daily_delivery(force=True, now=datetime(2026, 3, 30, 8, 0))

        self.assertTrue(result["ok"])
        self.assertTrue(result["sent"])
        self.assertEqual(result["message"], "日报发送成功")
        mock_send_telegram_message.assert_called_once_with("token-123", "12345", "<b>日报正文</b>", parse_mode="HTML")
        mock_finalize_digest_payload.assert_called_once_with(payload)
        self.assertEqual(mock_save_daily_state.call_count, 3)
        self.assertEqual(mock_save_daily_state.call_args_list[0].args[0]["pending_delivery"]["state"], "sending")
        self.assertEqual(mock_save_daily_state.call_args_list[1].args[0]["pending_delivery"]["state"], "sent_not_committed")
        self.assertIsNone(mock_save_daily_state.call_args_list[2].args[0]["pending_delivery"])
        self.assertEqual(mock_save_daily_state.call_args_list[2].args[0]["last_delivery_message_ids"], [101])
        self.assertEqual(result["delivery_meta"]["x_retry_triggered"], False)

    @patch.object(delivery, "build_delivery_status", autospec=True)
    @patch.object(delivery, "_save_daily_state", autospec=True)
    @patch.object(delivery, "_load_daily_state", autospec=True)
    @patch.object(delivery, "load_intel_config", autospec=True)
    def test_resolve_pending_daily_delivery_confirm_marks_sent_and_clears_pending(
        self,
        mock_load_intel_config,
        mock_load_daily_state,
        mock_save_daily_state,
        mock_build_delivery_status,
    ) -> None:
        mock_load_intel_config.return_value = {"timezone": "Asia/Shanghai"}
        mock_load_daily_state.return_value = (
            {},
            {
                "pending_delivery": {
                    "state": "sent_not_committed",
                    "digest_date": "2026-03-30",
                    "prepared_at": "2026-03-30T08:00:00+08:00",
                    "sent_at": "2026-03-30T08:00:05+08:00",
                    "message_fingerprint": "abc123",
                    "message_ids": [101, 102],
                }
            },
        )
        mock_build_delivery_status.return_value = {"ok": True, "delivery": {"pending_state": ""}}

        result = delivery.resolve_pending_daily_delivery("confirm")

        patch_payload = mock_save_daily_state.call_args.args[0]
        self.assertEqual(patch_payload["last_sent_date"], "2026-03-30")
        self.assertEqual(patch_payload["last_sent_at"], "2026-03-30T08:00:05+08:00")
        self.assertEqual(patch_payload["last_delivery_fingerprint"], "abc123")
        self.assertEqual(patch_payload["last_delivery_message_ids"], [101, 102])
        self.assertIsNone(patch_payload["pending_delivery"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "confirm")

    @patch.object(delivery, "build_delivery_status", autospec=True)
    @patch.object(delivery, "_save_daily_state", autospec=True)
    @patch.object(delivery, "_load_daily_state", autospec=True)
    @patch.object(delivery, "load_intel_config", autospec=True)
    def test_resolve_pending_daily_delivery_clear_allows_retry(
        self,
        mock_load_intel_config,
        mock_load_daily_state,
        mock_save_daily_state,
        mock_build_delivery_status,
    ) -> None:
        mock_load_intel_config.return_value = {"timezone": "Asia/Shanghai"}
        mock_load_daily_state.return_value = (
            {},
            {
                "pending_delivery": {
                    "state": "sending",
                    "digest_date": "2026-03-30",
                    "prepared_at": "2026-03-30T08:00:00+08:00",
                    "message_ids": [],
                }
            },
        )
        mock_build_delivery_status.return_value = {"ok": True, "delivery": {"pending_state": ""}}

        result = delivery.resolve_pending_daily_delivery("clear")

        patch_payload = mock_save_daily_state.call_args.args[0]
        self.assertEqual(patch_payload["last_error"], "")
        self.assertIsNone(patch_payload["pending_delivery"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "clear")

    @patch.object(delivery, "send_telegram_message", autospec=True)
    @patch.object(delivery, "_build_event_pool_payload", autospec=True)
    @patch.object(delivery, "_load_daily_state", autospec=True)
    @patch.object(delivery, "_auto_bind_chat", autospec=True)
    @patch.object(delivery, "_refresh_bot_username", autospec=True)
    @patch.object(delivery, "_load_runtime_context", autospec=True)
    def test_run_daily_delivery_waits_until_due_time(
        self,
        mock_load_runtime_context,
        mock_refresh_bot_username,
        mock_auto_bind_chat,
        mock_load_daily_state,
        mock_build_event_pool_payload,
        mock_send_telegram_message,
    ) -> None:
        cfg = _cfg_with_x()
        telegram = dict(cfg["telegram"])
        mock_load_runtime_context.return_value = (cfg, telegram, "token-123")
        mock_refresh_bot_username.return_value = cfg
        mock_auto_bind_chat.return_value = cfg
        mock_load_daily_state.return_value = ({}, {"last_sent_date": "", "last_sent_at": "", "last_attempt_at": "", "last_error": ""})

        result = delivery.run_daily_delivery(force=False, now=datetime(2026, 3, 30, 7, 30))

        self.assertTrue(result["ok"])
        self.assertFalse(result["sent"])
        self.assertIn("等待下次推送 2026-03-30 08:00 Asia/Shanghai", result["message"])
        mock_build_event_pool_payload.assert_not_called()
        mock_send_telegram_message.assert_not_called()

    @patch.object(delivery, "_save_daily_state", autospec=True)
    @patch.object(delivery, "send_telegram_message", autospec=True)
    @patch.object(delivery, "get_digest_message_html", autospec=True)
    @patch.object(delivery, "_build_event_pool_payload", autospec=True)
    @patch.object(delivery, "_load_daily_state", autospec=True)
    @patch.object(delivery, "_auto_bind_chat", autospec=True)
    @patch.object(delivery, "_refresh_bot_username", autospec=True)
    @patch.object(delivery, "_load_runtime_context", autospec=True)
    def test_run_daily_delivery_records_failure_when_send_fails(
        self,
        mock_load_runtime_context,
        mock_refresh_bot_username,
        mock_auto_bind_chat,
        mock_load_daily_state,
        mock_build_event_pool_payload,
        mock_get_digest_message_html,
        mock_send_telegram_message,
        mock_save_daily_state,
    ) -> None:
        cfg = _cfg_with_x()
        telegram = dict(cfg["telegram"])
        mock_load_runtime_context.return_value = (cfg, telegram, "token-123")
        mock_refresh_bot_username.return_value = cfg
        mock_auto_bind_chat.return_value = cfg
        mock_load_daily_state.return_value = ({}, {"last_sent_date": "", "last_sent_at": "", "last_attempt_at": "", "last_error": ""})
        mock_build_event_pool_payload.return_value = ({"message": "日报正文"}, {"x_retry_triggered": False})
        mock_get_digest_message_html.return_value = ""
        mock_send_telegram_message.side_effect = RuntimeError("telegram send failed")
        mock_save_daily_state.side_effect = [
            {
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
                "last_error": "",
                "pending_delivery": {"state": "sending"},
            },
            {
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
                "last_error": "telegram send failed",
                "pending_delivery": None,
            },
        ]

        result = delivery.run_daily_delivery(force=True, now=datetime(2026, 3, 30, 8, 0))

        self.assertFalse(result["ok"])
        self.assertFalse(result["sent"])
        self.assertEqual(result["message"], "日报发送失败")
        self.assertEqual(result["error"], "telegram send failed")
        self.assertEqual(mock_save_daily_state.call_count, 2)
        self.assertIsNone(mock_save_daily_state.call_args_list[1].args[0]["pending_delivery"])

    @patch.object(delivery, "send_telegram_message", autospec=True)
    @patch.object(delivery, "_build_event_pool_payload", autospec=True)
    @patch.object(delivery, "_load_daily_state", autospec=True)
    @patch.object(delivery, "_auto_bind_chat", autospec=True)
    @patch.object(delivery, "_refresh_bot_username", autospec=True)
    @patch.object(delivery, "_load_runtime_context", autospec=True)
    def test_run_daily_delivery_skips_when_pending_delivery_exists(
        self,
        mock_load_runtime_context,
        mock_refresh_bot_username,
        mock_auto_bind_chat,
        mock_load_daily_state,
        mock_build_event_pool_payload,
        mock_send_telegram_message,
    ) -> None:
        cfg = _cfg_with_x()
        telegram = dict(cfg["telegram"])
        mock_load_runtime_context.return_value = (cfg, telegram, "token-123")
        mock_refresh_bot_username.return_value = cfg
        mock_auto_bind_chat.return_value = cfg
        mock_load_daily_state.return_value = (
            {},
            {
                "last_sent_date": "",
                "last_sent_at": "",
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
                "last_error": "",
                "pending_delivery": {
                    "state": "sent_not_committed",
                    "digest_date": "2026-03-30",
                    "prepared_at": "2026-03-30T08:00:00+08:00",
                },
            },
        )

        result = delivery.run_daily_delivery(force=True, now=datetime(2026, 3, 30, 8, 10))

        self.assertTrue(result["ok"])
        self.assertFalse(result["sent"])
        self.assertIn("已暂停自动重发", result["message"])
        mock_build_event_pool_payload.assert_not_called()
        mock_send_telegram_message.assert_not_called()

    def test_inspect_daily_delivery_window_marks_overdue_and_fallback_due(self) -> None:
        cfg = _cfg_with_x()
        telegram = dict(cfg["telegram"])
        window = delivery.inspect_daily_delivery_window(
            cfg=cfg,
            telegram=telegram,
            token="token-123",
            daily_state={
                "last_sent_date": "",
                "last_sent_at": "",
                "last_error": "telegram timeout",
                "last_attempt_at": "2026-03-30T08:00:00+08:00",
            },
            now=datetime(2026, 3, 30, 8, 20),
        )

        self.assertTrue(window["is_due"])
        self.assertTrue(window["is_overdue"])
        self.assertTrue(window["fallback_due"])
        self.assertEqual(window["overdue_minutes"], 20)
        self.assertIn("自动补偿阈值", window["status_text"])

    @patch.object(delivery, "send_telegram_message", autospec=True)
    @patch.object(delivery, "_build_event_pool_payload", autospec=True)
    @patch.object(delivery, "_auto_bind_chat", autospec=True)
    @patch.object(delivery, "_refresh_bot_username", autospec=True)
    @patch.object(delivery, "_load_runtime_context", autospec=True)
    def test_send_test_telegram_message_uses_event_pool_preview(
        self,
        mock_load_runtime_context,
        mock_refresh_bot_username,
        mock_auto_bind_chat,
        mock_build_event_pool_payload,
        mock_send_telegram_message,
    ) -> None:
        cfg = _cfg_with_x()
        telegram = dict(cfg["telegram"])
        payload = {"digest_date": "2026-03-30", "message": "日报正文", "message_html": "<b>日报正文</b>", "build_stats": {}, "summary": {}}
        mock_load_runtime_context.return_value = (cfg, telegram, "token-123")
        mock_refresh_bot_username.return_value = cfg
        mock_auto_bind_chat.return_value = cfg
        mock_build_event_pool_payload.return_value = (payload, {"x_retry_triggered": False})

        result = delivery.send_test_telegram_message()

        self.assertTrue(result["ok"])
        self.assertEqual(result["message_source"], "event_pool_preview")
        mock_build_event_pool_payload.assert_called_once()
        self.assertFalse(mock_build_event_pool_payload.call_args.kwargs["respect_sent"])
        mock_send_telegram_message.assert_called_once()


if __name__ == "__main__":
    unittest.main()
