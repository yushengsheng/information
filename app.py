#!/usr/bin/env python3
"""Local web UI for the Binance wash-like volume monitor."""

from __future__ import annotations

import argparse
import atexit
import json
import os
import threading
import time
import webbrowser

from flask import Flask, jsonify, send_from_directory
from waitress import serve

from app_config import APP_VERSION, DEFAULT_PORT, PID_FILE, ROOT_DIR, UI_DIR
from monitor import format_clock
from routes.intel import register_intel_routes
from routes.monitor import register_monitor_routes
from services.intel.build_tasks import ManualDigestBuildTaskManager
from services.intel.scheduler import DailyIntelScheduler
from services.monitoring import MonitorManager

manager = MonitorManager()
intel_scheduler = DailyIntelScheduler()
digest_task_manager = ManualDigestBuildTaskManager()
app = Flask(__name__, static_folder=str(UI_DIR), static_url_path="/ui")
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

register_intel_routes(app, intel_scheduler, digest_task_manager)
register_monitor_routes(app, manager)


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/")
def index():
    return send_from_directory(UI_DIR, "index.html")


@app.get("/x")
def x_page():
    return send_from_directory(UI_DIR, "x.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "time": format_clock(), "app_version": APP_VERSION})


@app.errorhandler(500)
def handle_internal_error(_exc):
    return jsonify({"ok": False, "error": "internal server error"}), 500


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local Binance wash monitor UI.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--no-browser", action="store_true", help="Do not open the browser automatically")
    return parser


def write_pid_file(host: str, port: int) -> None:
    payload = {
        "pid": os.getpid(),
        "host": host,
        "port": port,
        "root_dir": str(ROOT_DIR),
        "started_at": int(time.time()),
    }
    PID_FILE.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def clear_pid_file() -> None:
    try:
        if not PID_FILE.exists():
            return
        payload = json.loads(PID_FILE.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and int(payload.get("pid", -1)) != os.getpid():
            return
    except Exception:
        pass

    try:
        PID_FILE.unlink()
    except OSError:
        pass


def main() -> int:
    args = build_parser().parse_args()
    url = f"http://{args.host}:{args.port}"
    write_pid_file(args.host, args.port)
    intel_scheduler.start()
    if not args.no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    serve(app, host=args.host, port=args.port, threads=16)
    return 0


atexit.register(clear_pid_file)
atexit.register(intel_scheduler.shutdown)
atexit.register(manager.shutdown)


if __name__ == "__main__":
    raise SystemExit(main())
