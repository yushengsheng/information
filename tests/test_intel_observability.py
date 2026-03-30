from __future__ import annotations

from datetime import datetime
import unittest

from services.intel.observability import build_observability_history_entry, build_observability_status


def _cfg_with_x() -> dict[str, object]:
    return {
        "fixed": {
            "crypto": {"x_queries": ["bitcoin etf"]},
            "world": {"x_queries": []},
            "hot": {"x_queries": []},
        },
        "custom": {"x_users": []},
    }


def _digest_with_counts(*, x: int, rss: int, reddit: int, fallback_items: int = 0, total_items: int = 0) -> dict[str, object]:
    return {
        "exists": True,
        "generated_at": "2026-03-30T08:00:00+08:00",
        "build_stats": {
            "raw_counts": {"crypto": x + rss + reddit, "world": 0, "hot": 0, "custom": 0, "persistent": 0},
            "selected_counts": {"crypto": max(x + rss + reddit, 1), "world": 0, "hot": 0, "custom": 0, "persistent": 0},
            "source_counts": {
                "crypto": {"x": x, "rss": rss, "reddit": reddit},
                "world": {},
                "persistent": {},
                "hot": {},
                "custom": {},
            },
            "summary": {
                "total_items": total_items,
                "fallback_items": fallback_items,
            },
        },
    }


def _opencli_status(
    *,
    installed: bool = True,
    connected: bool = True,
    auto_recover_on_demand: bool = False,
    status_stale: bool = False,
    status_age_seconds: int | None = None,
    connection_source: str = "daemon",
    last_status_error: str = "",
) -> dict[str, object]:
    return {
        "installed": installed,
        "connected": connected,
        "auto_recover_on_demand": auto_recover_on_demand,
        "status_stale": status_stale,
        "status_age_seconds": status_age_seconds,
        "connection_source": connection_source,
        "message": "",
        "hint": "",
        "last_status_error": last_status_error,
        "last_success_at": None,
        "chrome_window_count": 1,
    }


class IntelObservabilityTests(unittest.TestCase):
    def test_treats_first_collect_as_startup_info(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=4, rss=2, reddit=0, total_items=6),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "",
                "last_collect_error": "",
                "last_collect_message": "后台抓取中",
                "next_collect_due_at": "",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            now_ts=now_ts,
        )

        self.assertEqual(status["overview"]["level"], "info")
        self.assertIn("运行提示", status["overview"]["status_text"])
        alert = status["alerts"][0]
        self.assertEqual(alert["code"], "background_collect_starting")
        self.assertEqual(alert["severity"], "info")

    def test_flags_missing_x_and_rss_dominance(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=0, rss=8, reddit=0, total_items=8),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T07:30:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T09:30:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            now_ts=now_ts,
        )

        codes = {alert["code"] for alert in status["alerts"]}
        self.assertIn("x_source_missing", codes)
        self.assertIn("rss_dominant", codes)
        self.assertEqual(status["overview"]["level"], "critical")

    def test_flags_summary_fallback_and_stale_collect(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T12:00:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=4, rss=2, reddit=0, fallback_items=5, total_items=6),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T02:00:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T00:00:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            now_ts=now_ts,
        )

        codes = {alert["code"] for alert in status["alerts"]}
        self.assertIn("background_collect_stale", codes)
        self.assertIn("summary_fallback_high", codes)

    def test_flags_daily_delivery_overdue(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T12:00:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=4, rss=2, reddit=0, total_items=6),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T08:30:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T10:30:00+08:00",
                "delivery_overdue": True,
                "delivery_overdue_minutes": 18,
                "delivery_fallback_due": True,
                "delivery_fallback_attempted": False,
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            now_ts=now_ts,
        )

        alert_map = {alert["code"]: alert for alert in status["alerts"]}
        self.assertIn("daily_delivery_overdue", alert_map)
        self.assertEqual(alert_map["daily_delivery_overdue"]["severity"], "critical")

    def test_prefers_preview_task_result_over_latest_digest(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        latest_digest = _digest_with_counts(x=0, rss=8, reddit=0, total_items=8)
        task_snapshot = {
            "task": {
                "status": "succeeded",
                "updated_at": "2026-03-30T08:10:00+08:00",
                "result": _digest_with_counts(x=5, rss=1, reddit=0, total_items=6),
            }
        }

        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=latest_digest,
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T08:00:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T10:00:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot=task_snapshot,
            now_ts=now_ts,
        )

        codes = {alert["code"] for alert in status["alerts"]}
        self.assertNotIn("x_source_missing", codes)
        self.assertEqual(status["overview"]["payload_source"], "build_task")
        self.assertEqual(status["metrics"]["source_mix"]["x_total_configured"], 5)

    def test_flags_opencli_bridge_disconnected_for_x_pipeline(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=0, rss=2, reddit=1, total_items=3),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T08:30:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T10:30:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            opencli_status=_opencli_status(connected=False, last_status_error="bridge unavailable"),
            now_ts=now_ts,
        )

        codes = {alert["code"] for alert in status["alerts"]}
        self.assertIn("opencli_bridge_disconnected", codes)
        self.assertNotIn("x_source_missing", codes)
        self.assertEqual(status["metrics"]["opencli"]["state"], "disconnected")

    def test_treats_opencli_standby_as_non_fault(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=4, rss=2, reddit=0, total_items=6),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T08:30:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T10:30:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            opencli_status=_opencli_status(connected=False, auto_recover_on_demand=True),
            now_ts=now_ts,
        )

        codes = {alert["code"] for alert in status["alerts"]}
        self.assertNotIn("opencli_bridge_disconnected", codes)
        self.assertNotIn("opencli_status_stale", codes)
        self.assertEqual(status["metrics"]["opencli"]["state"], "standby")

    def test_summarizes_recent_repeated_alerts(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=0, rss=8, reddit=0, total_items=8),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T08:30:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T10:30:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            history_entries=[
                {"captured_at": now_ts - 300, "alert_codes": ["x_source_missing", "rss_dominant"], "level": "critical", "source": "background_collect"},
                {"captured_at": now_ts - 7200, "alert_codes": ["x_source_missing"], "level": "critical", "source": "background_collect"},
                {"captured_at": now_ts - 14000, "alert_codes": ["x_source_missing"], "level": "critical", "source": "background_collect"},
            ],
            now_ts=now_ts,
        )

        trend = status["metrics"]["trend"]
        self.assertEqual(trend["sample_count"], 3)
        self.assertEqual(trend["repeat_count"], 1)
        self.assertEqual(trend["repeat_items"][0]["code"], "x_source_missing")
        alert_map = {alert["code"]: alert for alert in status["alerts"]}
        self.assertEqual(alert_map["x_source_missing"]["recent_count"], "3")
        self.assertIn("trend_repeated_anomalies", alert_map)

    def test_build_observability_history_entry_carries_repeat_codes(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=0, rss=8, reddit=0, total_items=8),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "2026-03-30T08:30:00+08:00",
                "last_collect_error": "",
                "next_collect_due_at": "2026-03-30T10:30:00+08:00",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            history_entries=[
                {"captured_at": now_ts - 300, "alert_codes": ["x_source_missing"], "level": "critical", "source": "background_collect"},
                {"captured_at": now_ts - 7200, "alert_codes": ["x_source_missing"], "level": "critical", "source": "background_collect"},
            ],
            now_ts=now_ts,
        )

        entry = build_observability_history_entry(status, source="background_collect", now_ts=now_ts)

        self.assertEqual(entry["source"], "background_collect")
        self.assertEqual(entry["repeat_codes"]["x_source_missing"], 2)

    def test_build_observability_history_entry_ignores_info_only_alerts(self) -> None:
        now_ts = int(datetime.fromisoformat("2026-03-30T09:30:00+08:00").timestamp())
        status = build_observability_status(
            cfg=_cfg_with_x(),
            latest_digest=_digest_with_counts(x=4, rss=2, reddit=0, total_items=6),
            scheduler_status={
                "running": True,
                "collect_interval_seconds": 7200,
                "last_collect_at": "",
                "last_collect_error": "",
                "last_collect_message": "后台抓取中",
                "next_collect_due_at": "",
            },
            summary_status={"summary": {"mode": "ai_first", "ai_available": True, "active_mode": "ai"}},
            task_snapshot={"task": None},
            now_ts=now_ts,
        )

        entry = build_observability_history_entry(status, source="background_collect", now_ts=now_ts)

        self.assertEqual(entry["alert_codes"], [])
        self.assertEqual(entry["issue_count"], 0)


if __name__ == "__main__":
    unittest.main()
