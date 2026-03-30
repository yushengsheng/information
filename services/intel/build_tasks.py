from __future__ import annotations

import subprocess
import threading
import time
import uuid
from datetime import datetime

from app_config import APP_VERSION
from services.intel.delivery import build_manual_digest_payload
from services.intel.store import load_intel_build_task_state, save_intel_build_task_state

_ACTIVE_STATUSES = {"queued", "running"}
_TASK_RETENTION_DAYS = 3
_TASK_RETENTION_SECONDS = _TASK_RETENTION_DAYS * 24 * 60 * 60


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _parse_iso_ts(value: object) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return 0
    try:
        return int(parsed.timestamp())
    except Exception:
        return 0


def _normalize_task(task: object) -> dict[str, object] | None:
    if not isinstance(task, dict):
        return None
    normalized = dict(task)
    status = str(normalized.get("status") or "").strip().lower()
    if not status:
        return None
    normalized["status"] = status
    normalized["final"] = bool(normalized.get("final", False))
    normalized["respect_sent"] = bool(normalized.get("respect_sent", normalized["final"]))
    normalized["app_version"] = str(normalized.get("app_version") or APP_VERSION)
    return normalized


class ManualDigestBuildTaskManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        state = self._normalize_state(load_intel_build_task_state())
        mutated = False
        for task in state["tasks"]:
            if str(task.get("status")) not in _ACTIVE_STATUSES:
                continue
            finished_at = _now_iso()
            task["status"] = "failed"
            task["finished_at"] = finished_at
            task["updated_at"] = finished_at
            task["message"] = "服务重启导致任务中断，请重新生成日报。"
            task["error"] = "服务重启导致任务中断"
            task.pop("result", None)
            mutated = True
        self._save_state(state if mutated else state)

    def _task_anchor_ts(self, task: dict[str, object]) -> int:
        return max(
            _parse_iso_ts(task.get("updated_at")),
            _parse_iso_ts(task.get("finished_at")),
            _parse_iso_ts(task.get("submitted_at")),
        )

    def _task_sort_key(self, task: dict[str, object]) -> tuple[int, str]:
        return (self._task_anchor_ts(task), str(task.get("id") or ""))

    def _normalize_state(self, state: object) -> dict[str, object]:
        raw_state = state if isinstance(state, dict) else {}
        raw_tasks = raw_state.get("tasks") if isinstance(raw_state.get("tasks"), list) else []
        if not raw_tasks:
            legacy_task = raw_state.get("task")
            if isinstance(legacy_task, dict):
                raw_tasks = [legacy_task]

        deduped: dict[str, dict[str, object]] = {}
        cutoff_ts = int(time.time()) - _TASK_RETENTION_SECONDS
        for raw_task in raw_tasks:
            task = _normalize_task(raw_task)
            if not task:
                continue
            task_id = str(task.get("id") or "").strip()
            if not task_id:
                continue
            if self._task_anchor_ts(task) < cutoff_ts:
                continue
            existing = deduped.get(task_id)
            if existing is None or self._task_sort_key(task) >= self._task_sort_key(existing):
                deduped[task_id] = task

        tasks = sorted(deduped.values(), key=self._task_sort_key, reverse=True)
        latest_task_id = str(raw_state.get("latest_task_id") or "").strip()
        if latest_task_id and latest_task_id not in {str(task.get("id")) for task in tasks}:
            latest_task_id = ""
        if not latest_task_id and tasks:
            latest_task_id = str(tasks[0].get("id") or "")
        return {
            "version": 1,
            "retention_days": _TASK_RETENTION_DAYS,
            "latest_task_id": latest_task_id,
            "tasks": tasks,
        }

    def _load_state(self) -> dict[str, object]:
        return self._normalize_state(load_intel_build_task_state())

    def _save_state(self, state: dict[str, object]) -> dict[str, object]:
        normalized = self._normalize_state(state)
        save_intel_build_task_state(normalized)
        return normalized

    def _get_task_from_state(self, state: dict[str, object], task_id: str | None = None) -> dict[str, object] | None:
        tasks = state.get("tasks") if isinstance(state.get("tasks"), list) else []
        if task_id:
            for task in tasks:
                if str(task.get("id") or "") == task_id:
                    return task
        latest_task_id = str(state.get("latest_task_id") or "").strip()
        if latest_task_id:
            for task in tasks:
                if str(task.get("id") or "") == latest_task_id:
                    return task
        return tasks[0] if tasks else None

    def _load_task(self, task_id: str | None = None) -> dict[str, object] | None:
        return self._get_task_from_state(self._load_state(), task_id=task_id)

    def _strip_result(self, task: dict[str, object], *, include_result: bool) -> dict[str, object]:
        normalized = dict(task)
        result = normalized.get("result") if isinstance(normalized.get("result"), dict) else None
        if result:
            build_stats = result.get("build_stats") if isinstance(result.get("build_stats"), dict) else {}
            normalized["has_result"] = True
            normalized["result_digest_date"] = str(result.get("digest_date") or "")
            normalized["result_counts"] = dict(result.get("counts") or {}) if isinstance(result.get("counts"), dict) else {}
            normalized["result_timings_ms"] = dict(build_stats.get("timings_ms") or {}) if isinstance(build_stats.get("timings_ms"), dict) else {}
        else:
            normalized["has_result"] = False
            normalized["result_digest_date"] = ""
            normalized["result_counts"] = {}
            normalized["result_timings_ms"] = {}

        if not include_result:
            normalized.pop("result", None)
        return normalized

    def snapshot(self, *, include_result: bool = False) -> dict[str, object]:
        with self._lock:
            state = self._load_state()
            task = self._get_task_from_state(state)
            return {
                "ok": True,
                "app_version": APP_VERSION,
                "task": self._strip_result(task, include_result=include_result) if task else None,
                "active": bool(task and str(task.get("status")) in _ACTIVE_STATUSES),
                "history_count": len(state.get("tasks") if isinstance(state.get("tasks"), list) else []),
                "retention_days": _TASK_RETENTION_DAYS,
            }

    def submit(self, *, final: bool, respect_sent: bool | None = None) -> dict[str, object]:
        with self._lock:
            state = self._load_state()
            tasks = state.get("tasks") if isinstance(state.get("tasks"), list) else []
            existing = next((task for task in tasks if str(task.get("status")) in _ACTIVE_STATUSES), None)
            if existing:
                state["latest_task_id"] = str(existing.get("id") or "")
                self._save_state(state)
                return {
                    "ok": True,
                    "submitted": False,
                    "active": True,
                    "message": "已有日报生成任务在后台运行。",
                    "task": self._strip_result(existing, include_result=False),
                    "app_version": APP_VERSION,
                }

            task_id = uuid.uuid4().hex[:12]
            submitted_at = _now_iso()
            task = {
                "id": task_id,
                "kind": "final" if final else "preview",
                "status": "queued",
                "final": bool(final),
                "respect_sent": bool(respect_sent if isinstance(respect_sent, bool) else final),
                "submitted_at": submitted_at,
                "started_at": "",
                "finished_at": "",
                "updated_at": submitted_at,
                "message": "任务已提交，等待后台开始生成。",
                "error": "",
                "result": None,
                "app_version": APP_VERSION,
            }
            tasks.insert(0, task)
            state["tasks"] = tasks
            state["latest_task_id"] = task_id
            self._save_state(state)
            self._thread = threading.Thread(
                target=self._run_task,
                args=(task_id,),
                daemon=True,
                name=f"intel-digest-build-{task_id}",
            )
            self._thread.start()
            return {
                "ok": True,
                "submitted": True,
                "active": True,
                "message": "日报生成任务已提交到后台。",
                "task": self._strip_result(task, include_result=False),
                "app_version": APP_VERSION,
            }

    def _update_task(self, task_id: str, **patch: object) -> dict[str, object] | None:
        with self._lock:
            state = self._load_state()
            tasks = state.get("tasks") if isinstance(state.get("tasks"), list) else []
            index = next((idx for idx, task in enumerate(tasks) if str(task.get("id") or "") == task_id), -1)
            if index < 0:
                return None
            merged = {**tasks[index], **patch}
            merged["updated_at"] = _now_iso()
            tasks[index] = merged
            state["tasks"] = tasks
            state["latest_task_id"] = task_id
            saved_state = self._save_state(state)
            saved_task = self._get_task_from_state(saved_state, task_id=task_id)
            return saved_task

    def _fail_task(self, task_id: str, message: str, error: str) -> None:
        self._update_task(
            task_id,
            status="failed",
            finished_at=_now_iso(),
            message=message,
            error=error,
            result=None,
        )

    def _run_task(self, task_id: str) -> None:
        task = self._load_task()
        if not task or str(task.get("id")) != task_id:
            return

        final = bool(task.get("final", False))
        respect_sent = bool(task.get("respect_sent", final))
        started_at = _now_iso()
        self._update_task(
            task_id,
            status="running",
            started_at=started_at,
            message="正在抓取、聚合排序并生成摘要，页面会自动刷新结果。",
            error="",
        )

        try:
            payload = build_manual_digest_payload(final=final, respect_sent=respect_sent)
        except subprocess.TimeoutExpired:
            self._fail_task(task_id, "日报生成超时，请稍后重试。", "抓取或摘要超时")
            return
        except Exception as exc:
            self._fail_task(task_id, f"日报生成失败：{exc}", str(exc))
            return

        finished_at = _now_iso()
        total_ms = 0
        try:
            total_ms = int((payload.get("build_stats") or {}).get("timings_ms", {}).get("total") or 0)
        except Exception:
            total_ms = 0
        message = (
            f"正式日报已生成，用时约 {round(total_ms / 1000.0, 1)} 秒。"
            if final
            else f"预览日报已生成，用时约 {round(total_ms / 1000.0, 1)} 秒。"
        )
        self._update_task(
            task_id,
            status="succeeded",
            finished_at=finished_at,
            message=message,
            error="",
            result=payload,
        )

    def is_active(self) -> bool:
        state = self._load_state()
        tasks = state.get("tasks") if isinstance(state.get("tasks"), list) else []
        return any(str(task.get("status")) in _ACTIVE_STATUSES for task in tasks)
