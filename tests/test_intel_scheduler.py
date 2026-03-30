from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest.mock import Mock, patch


monitor_stub = types.ModuleType("monitor")
monitor_stub.format_clock = lambda: "2026-03-30 08:00:00"
sys.modules["monitor"] = monitor_stub

scheduler = importlib.import_module("services.intel.scheduler")


class IntelSchedulerTests(unittest.TestCase):
    @patch.object(scheduler.DailyIntelScheduler, "_record_observability_snapshot", autospec=True)
    @patch.object(scheduler, "_format_local_time", autospec=True)
    @patch.object(scheduler.time, "time", autospec=True)
    @patch.object(scheduler, "refresh_latest_digest_snapshot", autospec=True)
    def test_maybe_refresh_snapshot_success_updates_status_and_next_due(
        self,
        mock_refresh_latest_digest_snapshot,
        mock_time,
        mock_format_local_time,
        mock_record_observability_snapshot,
    ) -> None:
        mock_time.side_effect = [1000.0, 1010.0]
        mock_format_local_time.return_value = "next-at"
        mock_refresh_latest_digest_snapshot.return_value = {
            "generated_at": "2026-03-30T08:00:00+08:00",
            "build_stats": {"timings_ms": {"total": 2500}},
        }
        manager = scheduler.DailyIntelScheduler()

        manager._maybe_refresh_snapshot()

        status = manager.status()
        self.assertEqual(status["last_collect_at"], "2026-03-30T08:00:00+08:00")
        self.assertEqual(status["last_collect_error"], "")
        self.assertEqual(status["next_collect_due_at"], "next-at")
        self.assertIn("后台抓取完成", status["last_collect_message"])
        self.assertIn("2.5 秒", status["last_collect_message"])
        self.assertEqual(manager._next_collect_at, 1010.0 + scheduler.INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS)
        mock_record_observability_snapshot.assert_called_once_with(
            manager,
            latest_digest=mock_refresh_latest_digest_snapshot.return_value,
        )

    @patch.object(scheduler.DailyIntelScheduler, "_record_observability_snapshot", autospec=True)
    @patch.object(scheduler, "_format_local_time", autospec=True)
    @patch.object(scheduler.time, "time", autospec=True)
    @patch.object(scheduler, "refresh_latest_digest_snapshot", autospec=True)
    def test_maybe_refresh_snapshot_failure_sets_retry_window(
        self,
        mock_refresh_latest_digest_snapshot,
        mock_time,
        mock_format_local_time,
        mock_record_observability_snapshot,
    ) -> None:
        mock_time.side_effect = [1000.0, 1005.0]
        mock_format_local_time.return_value = "retry-at"
        mock_refresh_latest_digest_snapshot.side_effect = RuntimeError("boom")
        manager = scheduler.DailyIntelScheduler()

        manager._maybe_refresh_snapshot()

        status = manager.status()
        self.assertEqual(status["last_collect_error"], "boom")
        self.assertEqual(status["last_collect_message"], "后台抓取失败")
        self.assertEqual(status["next_collect_due_at"], "retry-at")
        self.assertEqual(manager._next_collect_at, 1005.0 + min(10 * 60, scheduler.INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS))
        mock_record_observability_snapshot.assert_called_once_with(manager)

    @patch.object(scheduler, "_format_local_time", autospec=True)
    @patch.object(scheduler.time, "time", autospec=True)
    @patch.object(scheduler, "refresh_latest_digest_snapshot", autospec=True)
    def test_maybe_refresh_snapshot_waits_until_next_due(
        self,
        mock_refresh_latest_digest_snapshot,
        mock_time,
        mock_format_local_time,
    ) -> None:
        mock_time.return_value = 1000.0
        mock_format_local_time.return_value = "future-at"
        manager = scheduler.DailyIntelScheduler()
        manager._next_collect_at = 1100.0

        manager._maybe_refresh_snapshot()

        status = manager.status()
        self.assertEqual(status["next_collect_due_at"], "future-at")
        mock_refresh_latest_digest_snapshot.assert_not_called()

    @patch.object(scheduler.DailyIntelScheduler, "_maybe_refresh_snapshot", autospec=True)
    @patch.object(scheduler.DailyIntelScheduler, "_launchd_managed_delivery_status", autospec=True)
    @patch.object(scheduler.DailyIntelScheduler, "_is_launchd_delivery_active", autospec=True)
    def test_tick_uses_launchd_status_when_active(
        self,
        mock_is_launchd_delivery_active,
        mock_launchd_managed_delivery_status,
        mock_maybe_refresh_snapshot,
    ) -> None:
        mock_is_launchd_delivery_active.return_value = True
        mock_launchd_managed_delivery_status.return_value = {
            "checked_at": "2026-03-30T08:00:00+08:00",
            "last_attempt_at": "2026-03-30T08:00:00+08:00",
            "last_sent_at": "2026-03-30T08:00:00+08:00",
            "last_error": "",
            "message": "今日已推送（2026-03-30）",
        }
        manager = scheduler.DailyIntelScheduler()

        manager._tick()

        status = manager.status()
        self.assertEqual(status["daily_delivery_mode"], "launchd")
        self.assertEqual(status["last_message"], "今日已推送（2026-03-30）")
        mock_maybe_refresh_snapshot.assert_called_once_with(manager)

    @patch.object(scheduler, "record_daily_fallback_attempt", autospec=True)
    @patch.object(scheduler, "run_daily_delivery", autospec=True)
    @patch.object(scheduler.DailyIntelScheduler, "_maybe_refresh_snapshot", autospec=True)
    @patch.object(scheduler.DailyIntelScheduler, "_launchd_managed_delivery_status", autospec=True)
    @patch.object(scheduler.DailyIntelScheduler, "_is_launchd_delivery_active", autospec=True)
    def test_tick_runs_launchd_fallback_when_delivery_is_overdue(
        self,
        mock_is_launchd_delivery_active,
        mock_launchd_managed_delivery_status,
        mock_maybe_refresh_snapshot,
        mock_run_daily_delivery,
        mock_record_daily_fallback_attempt,
    ) -> None:
        mock_is_launchd_delivery_active.return_value = True
        mock_launchd_managed_delivery_status.side_effect = [
            {
                "checked_at": "2026-03-30T08:20:00+08:00",
                "last_attempt_at": "",
                "last_sent_at": "",
                "last_error": "telegram timeout",
                "message": "今日日报已超过计划发送时间 20 分钟，已达到自动补偿阈值。",
                "delivery_date": "2026-03-30",
                "delivery_overdue": True,
                "delivery_overdue_minutes": 20,
                "delivery_fallback_due": True,
                "delivery_fallback_attempted": False,
                "delivery_fallback_last_attempt_at": "",
                "delivery_fallback_last_result": "",
            },
            {
                "checked_at": "2026-03-30T08:20:10+08:00",
                "last_attempt_at": "2026-03-30T08:20:05+08:00",
                "last_sent_at": "2026-03-30T08:20:05+08:00",
                "last_error": "",
                "message": "今日已推送（2026-03-30）",
                "delivery_date": "2026-03-30",
                "delivery_overdue": False,
                "delivery_overdue_minutes": 0,
                "delivery_fallback_due": False,
                "delivery_fallback_attempted": True,
                "delivery_fallback_last_attempt_at": "2026-03-30T08:20:05+08:00",
                "delivery_fallback_last_result": "日报发送成功",
            },
        ]
        mock_run_daily_delivery.return_value = {
            "checked_at": "2026-03-30T08:20:05+08:00",
            "last_attempt_at": "2026-03-30T08:20:05+08:00",
            "last_sent_at": "2026-03-30T08:20:05+08:00",
            "last_error": "",
            "message": "日报发送成功",
            "sent": True,
        }
        manager = scheduler.DailyIntelScheduler()

        manager._tick()

        status = manager.status()
        mock_run_daily_delivery.assert_called_once_with(force=True)
        mock_record_daily_fallback_attempt.assert_called_once_with(
            digest_date="2026-03-30",
            attempted_at="2026-03-30T08:20:05+08:00",
            result_message="日报发送成功",
        )
        self.assertEqual(status["daily_delivery_mode"], "launchd_fallback")
        self.assertEqual(status["last_message"], "LaunchAgent 超时后已自动补偿发送")
        mock_maybe_refresh_snapshot.assert_called_once_with(manager)

    @patch.object(scheduler, "save_observability_history_entry", autospec=True)
    @patch.object(scheduler, "build_observability_history_entry", autospec=True)
    @patch.object(scheduler, "build_observability_status", autospec=True)
    @patch.object(scheduler, "get_opencli_runtime_status", autospec=True)
    def test_record_observability_snapshot_builds_and_saves_entry(
        self,
        mock_get_opencli_runtime_status,
        mock_build_observability_status,
        mock_build_observability_history_entry,
        mock_save_observability_history_entry,
    ) -> None:
        manager = scheduler.DailyIntelScheduler()
        manager._set_status(last_collect_message="后台抓取完成")
        latest_digest = {"generated_at": "2026-03-30T08:00:00+08:00"}
        mock_get_opencli_runtime_status.return_value = {"installed": True, "connected": True}
        mock_build_observability_status.return_value = {"ok": True, "overview": {"level": "ok"}, "metrics": {}, "alerts": []}
        mock_build_observability_history_entry.return_value = {"captured_at": 123}

        manager._record_observability_snapshot(latest_digest=latest_digest)

        mock_build_observability_status.assert_called_once()
        self.assertEqual(mock_build_observability_status.call_args.kwargs["latest_digest"], latest_digest)
        self.assertEqual(mock_build_observability_status.call_args.kwargs["scheduler_status"]["last_collect_message"], "后台抓取完成")
        mock_build_observability_history_entry.assert_called_once_with(
            mock_build_observability_status.return_value,
            source="background_collect",
        )
        mock_save_observability_history_entry.assert_called_once_with({"captured_at": 123})


if __name__ == "__main__":
    unittest.main()
