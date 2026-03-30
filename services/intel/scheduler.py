from __future__ import annotations

import os
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from app_config import INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS
from services.intel.delivery import build_delivery_status, record_daily_fallback_attempt, run_daily_delivery
from services.intel.digest import refresh_latest_digest_snapshot
from services.intel.observability import build_observability_history_entry, build_observability_status
from services.intel.opencli import get_opencli_runtime_status
from services.intel.store import save_observability_history_entry

INTEL_DAILY_LAUNCHD_LABEL = "com.inverse.intel.daily"
INTEL_DAILY_LAUNCHD_PLIST = Path.home() / "Library/LaunchAgents" / f"{INTEL_DAILY_LAUNCHD_LABEL}.plist"
LAUNCHD_STATUS_CACHE_SECONDS = 60


def _format_local_time(timestamp: float) -> str:
    if timestamp <= 0:
        return ""
    return datetime.fromtimestamp(timestamp).astimezone().isoformat(timespec="seconds")


class DailyIntelScheduler:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._next_collect_at = 0.0
        self._launchd_managed_cache = False
        self._launchd_checked_at = 0.0
        self._status: dict[str, object] = {
            "running": False,
            "last_check_at": "",
            "last_attempt_at": "",
            "last_sent_at": "",
            "last_error": "",
            "last_message": "未启动",
            "daily_delivery_mode": "in_app",
            "collect_interval_seconds": INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS,
            "last_collect_at": "",
            "last_collect_error": "",
            "last_collect_message": "后台抓取未启动",
            "next_collect_due_at": "",
            "delivery_overdue": False,
            "delivery_overdue_minutes": 0,
            "delivery_fallback_due": False,
            "delivery_fallback_attempted": False,
            "delivery_fallback_last_attempt_at": "",
            "delivery_fallback_last_result": "",
            "fallback_auto_triggered": False,
            "fallback_result_message": "",
        }

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._next_collect_at = 0.0
            self._thread = threading.Thread(target=self._run_loop, name="intel-daily-scheduler", daemon=True)
            self._status["running"] = True
            self._status["last_message"] = "调度器运行中"
            self._status["daily_delivery_mode"] = "launchd" if self._is_launchd_delivery_active(force_refresh=True) else "in_app"
            self._status["collect_interval_seconds"] = INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS
            self._status["last_collect_message"] = "等待启动后的首次后台抓取"
            self._status["next_collect_due_at"] = ""
            self._thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        thread = None
        with self._lock:
            thread = self._thread
        if thread:
            thread.join(timeout=5.0)
        with self._lock:
            self._status["running"] = False
            self._status["last_message"] = "调度器已停止"

    def status(self) -> dict[str, object]:
        with self._lock:
            return dict(self._status)

    def _set_status(self, **patch: object) -> None:
        with self._lock:
            self._status.update(patch)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as exc:
                now_text = datetime.now().astimezone().isoformat(timespec="seconds")
                self._set_status(
                    last_check_at=now_text,
                    last_error=str(exc),
                    last_message="调度检查失败",
                )
            if self._stop_event.wait(20):
                break

    def _is_launchd_delivery_active(self, *, force_refresh: bool = False) -> bool:
        now_ts = time.time()
        if not force_refresh and self._launchd_checked_at and now_ts - self._launchd_checked_at < LAUNCHD_STATUS_CACHE_SECONDS:
            return self._launchd_managed_cache

        active = False
        if INTEL_DAILY_LAUNCHD_PLIST.exists():
            try:
                proc = subprocess.run(
                    ["launchctl", "print", f"gui/{os.getuid()}/{INTEL_DAILY_LAUNCHD_LABEL}"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                active = proc.returncode == 0
            except Exception:
                active = False

        self._launchd_managed_cache = active
        self._launchd_checked_at = now_ts
        return active

    def _launchd_managed_delivery_status(self) -> dict[str, object]:
        payload = build_delivery_status()
        delivery = payload.get("delivery") if isinstance(payload.get("delivery"), dict) else {}
        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        last_sent_date = str(delivery.get("last_sent_date") or "")
        pending_state = str(delivery.get("pending_state") or "").strip().lower()
        pending_digest_date = str(delivery.get("pending_digest_date") or "").strip()
        today = str(delivery.get("today") or datetime.now().astimezone().strftime("%Y-%m-%d"))
        message = "正式推送由 LaunchAgent 接管"
        if pending_digest_date == today and pending_state in {"sending", "sent_not_committed"}:
            if pending_state == "sent_not_committed":
                message = "今日日报已发出，但本地确认未完成"
            else:
                message = "今日日报状态待确认，已暂停自动重发"
        elif last_sent_date == today:
            message = f"今日已推送（{today}）"
        elif bool(delivery.get("is_overdue")):
            message = str(delivery.get("status_text") or "今日日报已超时，仍未确认发送")

        return {
            "checked_at": now_text,
            "last_attempt_at": str(delivery.get("last_attempt_at") or ""),
            "last_sent_at": str(delivery.get("last_sent_at") or ""),
            "last_error": str(delivery.get("last_error") or ""),
            "message": message,
            "delivery_date": today,
            "delivery_overdue": bool(delivery.get("is_overdue")),
            "delivery_overdue_minutes": max(int(delivery.get("overdue_minutes") or 0), 0),
            "delivery_fallback_due": bool(delivery.get("fallback_due")),
            "delivery_fallback_attempted": bool(delivery.get("fallback_attempted_today")),
            "delivery_fallback_last_attempt_at": str(delivery.get("fallback_last_attempt_at") or ""),
            "delivery_fallback_last_result": str(delivery.get("fallback_last_result") or ""),
        }

    def _tick(self) -> None:
        if self._is_launchd_delivery_active():
            result = self._launchd_managed_delivery_status()
            delivery_mode = "launchd"
            if bool(result.get("delivery_fallback_due")):
                fallback_result = run_daily_delivery(force=True)
                record_daily_fallback_attempt(
                    digest_date=str(result.get("delivery_date") or ""),
                    attempted_at=str(fallback_result.get("checked_at") or ""),
                    result_message=str(fallback_result.get("message") or ""),
                )
                result = self._launchd_managed_delivery_status()
                result["fallback_auto_triggered"] = True
                result["fallback_result_message"] = str(fallback_result.get("message") or "")
                if fallback_result.get("sent"):
                    delivery_mode = "launchd_fallback"
                    result["message"] = "LaunchAgent 超时后已自动补偿发送"
                else:
                    result["message"] = f"LaunchAgent 超时后已自动补偿一次：{fallback_result.get('message') or '等待人工处理'}"
        else:
            result = run_daily_delivery()
            delivery_mode = "in_app"
        self._set_status(
            last_check_at=str(result.get("checked_at") or ""),
            last_attempt_at=str(result.get("last_attempt_at") or ""),
            last_sent_at=str(result.get("last_sent_at") or ""),
            last_error=str(result.get("last_error") or ""),
            last_message=str(result.get("message") or ""),
            daily_delivery_mode=delivery_mode,
            delivery_overdue=bool(result.get("delivery_overdue")),
            delivery_overdue_minutes=max(int(result.get("delivery_overdue_minutes") or 0), 0),
            delivery_fallback_due=bool(result.get("delivery_fallback_due")),
            delivery_fallback_attempted=bool(result.get("delivery_fallback_attempted")),
            delivery_fallback_last_attempt_at=str(result.get("delivery_fallback_last_attempt_at") or ""),
            delivery_fallback_last_result=str(result.get("delivery_fallback_last_result") or ""),
            fallback_auto_triggered=bool(result.get("fallback_auto_triggered")),
            fallback_result_message=str(result.get("fallback_result_message") or ""),
        )
        self._maybe_refresh_snapshot()

    def _maybe_refresh_snapshot(self) -> None:
        now_ts = time.time()
        if self._next_collect_at and now_ts < self._next_collect_at:
            self._set_status(next_collect_due_at=_format_local_time(self._next_collect_at))
            return

        started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        self._set_status(
            last_collect_error="",
            last_collect_message="后台抓取中",
            next_collect_due_at="",
        )
        try:
            payload = refresh_latest_digest_snapshot()
        except Exception as exc:
            retry_delay = min(10 * 60, INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS)
            self._next_collect_at = time.time() + retry_delay
            self._set_status(
                last_collect_at=started_at,
                last_collect_error=str(exc),
                last_collect_message="后台抓取失败",
                next_collect_due_at=_format_local_time(self._next_collect_at),
            )
            self._record_observability_snapshot()
            return

        self._next_collect_at = time.time() + INTEL_BACKGROUND_FETCH_INTERVAL_SECONDS
        timings = payload.get("build_stats") if isinstance(payload.get("build_stats"), dict) else {}
        timings_ms = timings.get("timings_ms") if isinstance(timings.get("timings_ms"), dict) else {}
        total_ms = int(timings_ms.get("total", 0)) if isinstance(timings_ms.get("total", 0), (int, float)) else 0
        duration_text = f"{total_ms / 1000:.1f} 秒" if total_ms > 0 else "-"
        self._set_status(
            last_collect_at=str(payload.get("generated_at") or started_at),
            last_collect_error="",
            last_collect_message=f"后台抓取完成（每 2 小时 1 次，用时 {duration_text}）",
            next_collect_due_at=_format_local_time(self._next_collect_at),
        )
        self._record_observability_snapshot(latest_digest=payload)

    def _record_observability_snapshot(self, latest_digest: dict[str, object] | None = None) -> None:
        try:
            opencli_status = get_opencli_runtime_status()
        except Exception as exc:
            opencli_status = {"ok": False, "status_error": str(exc)}

        try:
            status = build_observability_status(
                latest_digest=latest_digest,
                scheduler_status=self.status(),
                opencli_status=opencli_status,
            )
            entry = build_observability_history_entry(status, source="background_collect")
            save_observability_history_entry(entry)
        except Exception:
            return
