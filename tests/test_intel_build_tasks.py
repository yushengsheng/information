from __future__ import annotations

import importlib
import subprocess
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch


monitor_stub = types.ModuleType("monitor")
monitor_stub.format_clock = lambda: "2026-03-30 08:00:00"
sys.modules["monitor"] = monitor_stub

build_tasks = importlib.import_module("services.intel.build_tasks")


class IntelBuildTaskTests(unittest.TestCase):
    @patch.object(build_tasks, "save_intel_build_task_state", autospec=True)
    @patch.object(build_tasks, "load_intel_build_task_state", autospec=True)
    def test_init_marks_interrupted_tasks_failed_after_restart(
        self,
        mock_load_intel_build_task_state,
        mock_save_intel_build_task_state,
    ) -> None:
        mock_load_intel_build_task_state.return_value = {
            "tasks": [
                {
                    "id": "queued-task",
                    "status": "queued",
                    "submitted_at": "2026-03-30T07:59:00+08:00",
                    "updated_at": "2026-03-30T07:59:00+08:00",
                },
                {
                    "id": "running-task",
                    "status": "running",
                    "submitted_at": "2026-03-30T07:58:00+08:00",
                    "updated_at": "2026-03-30T07:58:30+08:00",
                },
                {
                    "id": "done-task",
                    "status": "succeeded",
                    "submitted_at": "2026-03-30T07:00:00+08:00",
                    "updated_at": "2026-03-30T07:02:00+08:00",
                },
            ]
        }

        build_tasks.ManualDigestBuildTaskManager()

        saved_state = mock_save_intel_build_task_state.call_args.args[0]
        tasks = {task["id"]: task for task in saved_state["tasks"]}
        self.assertEqual(tasks["queued-task"]["status"], "failed")
        self.assertEqual(tasks["running-task"]["status"], "failed")
        self.assertEqual(tasks["done-task"]["status"], "succeeded")
        self.assertIn("服务重启导致任务中断", tasks["queued-task"]["error"])

    @patch.object(build_tasks.threading, "Thread", autospec=True)
    @patch.object(build_tasks, "save_intel_build_task_state", autospec=True)
    @patch.object(build_tasks, "load_intel_build_task_state", autospec=True)
    @patch.object(build_tasks.uuid, "uuid4", autospec=True)
    def test_submit_creates_background_preview_task(
        self,
        mock_uuid4,
        mock_load_intel_build_task_state,
        mock_save_intel_build_task_state,
        mock_thread_cls,
    ) -> None:
        mock_uuid4.return_value = SimpleNamespace(hex="previewtask123456")
        mock_load_intel_build_task_state.return_value = {"tasks": []}
        fake_thread = Mock()
        mock_thread_cls.return_value = fake_thread
        manager = build_tasks.ManualDigestBuildTaskManager()

        result = manager.submit(final=False)

        self.assertTrue(result["submitted"])
        self.assertTrue(result["active"])
        self.assertEqual(result["task"]["kind"], "preview")
        self.assertEqual(result["task"]["status"], "queued")
        fake_thread.start.assert_called_once()

    @patch.object(build_tasks.threading, "Thread", autospec=True)
    @patch.object(build_tasks, "save_intel_build_task_state", autospec=True)
    @patch.object(build_tasks, "load_intel_build_task_state", autospec=True)
    def test_submit_reuses_existing_active_task(
        self,
        mock_load_intel_build_task_state,
        mock_save_intel_build_task_state,
        mock_thread_cls,
    ) -> None:
        mock_load_intel_build_task_state.return_value = {"tasks": []}
        manager = build_tasks.ManualDigestBuildTaskManager()
        active_task = {
            "id": "active-1",
            "status": "running",
            "kind": "preview",
            "submitted_at": "2026-03-30T07:59:00+08:00",
            "updated_at": "2026-03-30T08:00:00+08:00",
            "final": False,
            "respect_sent": False,
            "message": "running",
            "error": "",
        }

        with patch.object(manager, "_load_state", autospec=False, return_value={"tasks": [active_task], "latest_task_id": "active-1"}), patch.object(
            manager,
            "_save_state",
            autospec=False,
            return_value={"tasks": [active_task], "latest_task_id": "active-1"},
        ):
            result = manager.submit(final=False)

        self.assertFalse(result["submitted"])
        self.assertTrue(result["active"])
        self.assertEqual(result["task"]["id"], "active-1")
        mock_thread_cls.assert_not_called()

    @patch.object(build_tasks, "build_manual_digest_payload", autospec=True)
    @patch.object(build_tasks, "save_intel_build_task_state", autospec=True)
    @patch.object(build_tasks, "load_intel_build_task_state", autospec=True)
    def test_run_task_success_transitions_to_succeeded(
        self,
        mock_load_intel_build_task_state,
        mock_save_intel_build_task_state,
        mock_build_manual_digest_payload,
    ) -> None:
        mock_load_intel_build_task_state.return_value = {"tasks": []}
        mock_build_manual_digest_payload.return_value = {"build_stats": {"timings_ms": {"total": 2300}}}
        manager = build_tasks.ManualDigestBuildTaskManager()
        task = {"id": "task-1", "final": False, "respect_sent": False}

        with patch.object(manager, "_load_task", autospec=False, return_value=task), patch.object(
            manager,
            "_update_task",
            autospec=False,
            return_value=None,
        ) as mock_update_task:
            manager._run_task("task-1")

        self.assertEqual(mock_update_task.call_args_list[0].kwargs["status"], "running")
        self.assertEqual(mock_update_task.call_args_list[1].kwargs["status"], "succeeded")
        self.assertEqual(mock_update_task.call_args_list[1].kwargs["result"], {"build_stats": {"timings_ms": {"total": 2300}}})

    @patch.object(build_tasks, "build_manual_digest_payload", autospec=True)
    @patch.object(build_tasks, "save_intel_build_task_state", autospec=True)
    @patch.object(build_tasks, "load_intel_build_task_state", autospec=True)
    def test_run_task_timeout_marks_failed(
        self,
        mock_load_intel_build_task_state,
        mock_save_intel_build_task_state,
        mock_build_manual_digest_payload,
    ) -> None:
        mock_load_intel_build_task_state.return_value = {"tasks": []}
        mock_build_manual_digest_payload.side_effect = subprocess.TimeoutExpired(cmd="build", timeout=30)
        manager = build_tasks.ManualDigestBuildTaskManager()
        task = {"id": "task-timeout", "final": False, "respect_sent": False}

        with patch.object(manager, "_load_task", autospec=False, return_value=task), patch.object(
            manager,
            "_fail_task",
            autospec=False,
            return_value=None,
        ) as mock_fail_task:
            manager._run_task("task-timeout")

        mock_fail_task.assert_called_once()
        self.assertIn("日报生成超时", mock_fail_task.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
