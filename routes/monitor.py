from __future__ import annotations

from flask import Flask, jsonify, request

from app_config import APP_VERSION
from services.monitoring import MonitorManager, parse_config, parse_symbols


def register_monitor_routes(app: Flask, manager: MonitorManager) -> None:
    @app.get("/api/state")
    def api_state():
        return jsonify(manager.state())

    @app.post("/api/start")
    def api_start():
        try:
            payload = request.get_json(silent=True) or {}
            config = parse_config(payload)
            manager.restart(config)
            return jsonify({"ok": True, "state": manager.state()})
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/add-symbols")
    def api_add_symbols():
        try:
            payload = request.get_json(silent=True) or {}
            symbols = parse_symbols(payload.get("symbols", ""))
            result = manager.add_symbols(symbols)
            return jsonify({"ok": True, "result": result, "state": manager.state()})
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except RuntimeError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/remove-symbols")
    def api_remove_symbols():
        try:
            payload = request.get_json(silent=True) or {}
            symbols = parse_symbols(payload.get("symbols", ""))
            result = manager.remove_symbols(symbols)
            return jsonify({"ok": True, "result": result, "state": manager.state()})
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except RuntimeError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/sync-symbols")
    def api_sync_symbols():
        try:
            payload = request.get_json(silent=True) or {}
            symbols = parse_symbols(payload.get("symbols", ""), allow_empty=True)
            result = manager.sync_symbols(symbols)
            return jsonify({"ok": True, "result": result, "state": manager.state(), "app_version": APP_VERSION})
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        except RuntimeError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    @app.post("/api/stop")
    def api_stop():
        was_active = manager.stop(wait=False)
        return jsonify({"ok": True, "was_active": was_active, "state": manager.state(), "app_version": APP_VERSION})
