from __future__ import annotations

import subprocess

from flask import Flask, jsonify, request

from app_config import APP_VERSION
from services.intel.ai import build_summary_status
from services.intel.delivery import (
    build_delivery_status,
    resolve_and_bind_latest_chat,
    resolve_pending_daily_delivery,
    send_test_telegram_message,
)
from services.intel.observability import build_observability_status
from services.intel.opencli import get_opencli_runtime_status, get_opencli_status
from services.intel.store import load_intel_config, load_latest_digest, load_latest_sent_digest, save_intel_config
from services.intel.text import normalize_text
from services.intel.x_feed import build_x_feed_payload


def register_intel_routes(app: Flask, intel_scheduler, digest_task_manager) -> None:
    @app.get("/api/opencli/status")
    def api_opencli_status():
        try:
            mode = normalize_text(request.args.get("mode")).lower()
            payload = get_opencli_status() if mode in {"doctor", "deep", "full"} else get_opencli_runtime_status()
            payload["check_mode"] = "doctor" if mode in {"doctor", "deep", "full"} else "runtime"
            payload["app_version"] = APP_VERSION
            return jsonify(payload)
        except subprocess.TimeoutExpired:
            return jsonify({"ok": False, "error": "opencli 状态检查超时，请稍后重试", "app_version": APP_VERSION}), 504
        except Exception as exc:
            return jsonify({"ok": False, "error": f"opencli 状态检查失败: {exc}", "app_version": APP_VERSION}), 500

    @app.post("/api/x/fetch")
    def api_x_fetch():
        try:
            payload = request.get_json(silent=True) or {}
            result, status = build_x_feed_payload(payload)
            result["app_version"] = APP_VERSION
            return jsonify(result), status
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except RuntimeError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except subprocess.TimeoutExpired:
            return jsonify({"ok": False, "error": "opencli 执行超时，请检查网络与扩展连接"}), 504
        except Exception as exc:
            return jsonify({"ok": False, "error": f"X 信息获取失败: {exc}"}), 500

    @app.get("/api/intel/config")
    def api_intel_config_get():
        return jsonify({"ok": True, "config": load_intel_config(), "app_version": APP_VERSION})

    @app.post("/api/intel/config")
    def api_intel_config_save():
        try:
            payload = request.get_json(silent=True) or {}
            if not isinstance(payload, dict):
                payload = {}
            patch = payload.get("config", payload)
            if not isinstance(patch, dict):
                patch = {}
            config = save_intel_config(patch)
            return jsonify({"ok": True, "config": config, "app_version": APP_VERSION})
        except Exception as exc:
            return jsonify({"ok": False, "error": f"保存配置失败: {exc}"}), 500

    @app.post("/api/intel/digest/build")
    def api_intel_digest_build():
        try:
            payload = request.get_json(silent=True) or {}
            final = bool(payload.get("final", False))
            if final:
                return jsonify({"ok": False, "error": "手动正式版已移除，目前只保留预览日报。", "app_version": APP_VERSION}), 400
            respect_sent_raw = payload.get("respect_sent")
            respect_sent = bool(respect_sent_raw) if isinstance(respect_sent_raw, bool) else None
            result = digest_task_manager.submit(final=final, respect_sent=respect_sent)
            return jsonify(result), 202
        except Exception as exc:
            return jsonify({"ok": False, "error": f"提交日报任务失败: {exc}", "app_version": APP_VERSION}), 500

    @app.get("/api/intel/digest/build/status")
    def api_intel_digest_build_status():
        try:
            include_result = normalize_text(request.args.get("include_result")).lower() in {"1", "true", "yes", "full"}
            return jsonify(digest_task_manager.snapshot(include_result=include_result))
        except Exception as exc:
            return jsonify({"ok": False, "error": f"读取日报任务状态失败: {exc}", "app_version": APP_VERSION}), 500

    @app.get("/api/intel/digest/latest")
    def api_intel_digest_latest():
        mode = normalize_text(request.args.get("mode", "display")).lower()
        selected_date = normalize_text(request.args.get("date"))
        if mode == "final" and not selected_date:
            latest = load_latest_sent_digest() or {"ok": True, "exists": False, "selected_date": "", "available_dates": []}
        else:
            latest = load_latest_digest(selected_date or None)
        latest.setdefault("ok", True)
        stored_app_version = normalize_text(latest.get("app_version"))
        if stored_app_version and stored_app_version != APP_VERSION:
            latest["digest_app_version"] = stored_app_version
        latest["app_version"] = APP_VERSION

        if mode == "final":
            return jsonify(latest)

        latest["mode"] = "display"
        return jsonify(latest)

    @app.get("/api/intel/telegram/status")
    def api_intel_telegram_status():
        return jsonify(build_delivery_status(intel_scheduler.status()))

    @app.post("/api/intel/telegram/pending-delivery")
    def api_intel_telegram_pending_delivery():
        try:
            payload = request.get_json(silent=True) or {}
            if not isinstance(payload, dict):
                payload = {}
            action = payload.get("action")
            result = resolve_pending_daily_delivery(action)
            if not result.get("ok"):
                return jsonify(result), 400
            return jsonify(result)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc), "app_version": APP_VERSION}), 400
        except Exception as exc:
            return jsonify({"ok": False, "error": f"处理待确认日报失败: {exc}", "app_version": APP_VERSION}), 500

    @app.get("/api/intel/summary/status")
    def api_intel_summary_status():
        payload = build_summary_status()
        payload["app_version"] = APP_VERSION
        return jsonify(payload)

    @app.get("/api/intel/observability/status")
    def api_intel_observability_status():
        try:
            try:
                opencli_status = get_opencli_runtime_status()
            except Exception as exc:
                opencli_status = {"ok": False, "status_error": str(exc)}
            payload = build_observability_status(
                scheduler_status=intel_scheduler.status(),
                task_snapshot=digest_task_manager.snapshot(include_result=True),
                opencli_status=opencli_status,
            )
            payload["app_version"] = APP_VERSION
            return jsonify(payload)
        except Exception as exc:
            return jsonify({"ok": False, "error": f"读取运行诊断失败: {exc}", "app_version": APP_VERSION}), 500

    @app.post("/api/intel/telegram/resolve-chat")
    def api_intel_telegram_resolve_chat():
        try:
            result = resolve_and_bind_latest_chat()
            if not result.get("ok"):
                return jsonify(result), 400
            return jsonify(result)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc), "app_version": APP_VERSION}), 400
        except Exception as exc:
            return jsonify({"ok": False, "error": f"识别 Telegram 会话失败: {exc}", "app_version": APP_VERSION}), 500

    @app.post("/api/intel/telegram/test-send")
    def api_intel_telegram_test_send():
        try:
            return jsonify(send_test_telegram_message())
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc), "app_version": APP_VERSION}), 400
        except RuntimeError as exc:
            return jsonify({"ok": False, "error": str(exc), "app_version": APP_VERSION}), 400
        except Exception as exc:
            return jsonify({"ok": False, "error": f"Telegram 测试发送失败: {exc}", "app_version": APP_VERSION}), 500
