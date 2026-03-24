#!/usr/bin/env python3
"""Local web UI for the Binance wash-like volume monitor."""

from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import os
import re
import threading
import time
import traceback
import webbrowser
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request, send_from_directory

from monitor import (
    Thresholds,
    create_monitor,
    format_clock,
    format_window,
    report_loop,
    runtime_guard,
    stream_loop,
)

ROOT_DIR = Path(__file__).resolve().parent
UI_DIR = ROOT_DIR / "ui"
DEFAULT_PORT = 8765
PID_FILE = ROOT_DIR / ".monitor_ui.pid"
APP_VERSION = "20260324-sync-1"


@dataclass(slots=True)
class SessionConfig:
    symbols: list[str] = field(default_factory=lambda: ["NIGHTUSDT"])
    windows_seconds: list[int] = field(default_factory=lambda: [300, 600])
    report_interval: int = 10
    baseline_minutes: int = 120
    runtime_seconds: int = 0
    thresholds: Thresholds = field(default_factory=Thresholds)

    def to_dict(self) -> dict[str, object]:
        return {
            "symbols": self.symbols,
            "symbols_text": ",".join(self.symbols),
            "windows_seconds": self.windows_seconds,
            "windows_minutes": [window // 60 for window in self.windows_seconds],
            "window_labels": [format_window(window) for window in self.windows_seconds],
            "report_interval": self.report_interval,
            "baseline_minutes": self.baseline_minutes,
            "runtime_seconds": self.runtime_seconds,
        }


def parse_symbols(value: object) -> list[str]:
    if isinstance(value, str):
        raw_tokens = re.split(r"[\s,，;；]+", value.strip())
        items = [token for token in raw_tokens if token]
    elif isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        raise ValueError("币种列表请输入逗号分隔内容，例如 NIGHTUSDT,BTCUSDT")

    symbols: list[str] = []
    seen: set[str] = set()
    for item in items:
        symbol = item.upper()
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)

    if not symbols:
        raise ValueError("至少填写一个币种")
    return symbols


def parse_windows_minutes(value: object) -> list[int]:
    if isinstance(value, str):
        tokens = [token.strip() for token in value.replace("，", ",").split(",")]
        items = [token for token in tokens if token]
    elif isinstance(value, list):
        items = value
    else:
        raise ValueError("持续时间请输入逗号分隔的分钟数，例如 5,10")

    windows_minutes: list[int] = []
    for item in items:
        minutes = int(item)
        if minutes <= 0:
            raise ValueError("持续时间必须是正整数分钟")
        windows_minutes.append(minutes)

    unique_seconds = sorted({minutes * 60 for minutes in windows_minutes})
    if not unique_seconds:
        raise ValueError("至少填写一个持续时间")
    return unique_seconds


def parse_runtime(value: object) -> int:
    if value in (None, "", 0, "0"):
        return 0
    runtime_seconds = int(value)
    if runtime_seconds < 0:
        raise ValueError("自动停止秒数不能小于 0")
    return runtime_seconds


def parse_config(payload: dict[str, object]) -> SessionConfig:
    report_interval = int(payload.get("report_interval", 10))
    if report_interval <= 0:
        raise ValueError("刷新间隔必须大于 0 秒")

    baseline_minutes = int(payload.get("baseline_minutes", 120))
    if baseline_minutes <= 0:
        raise ValueError("基线分钟数必须大于 0")

    return SessionConfig(
        symbols=parse_symbols(payload.get("symbols", "NIGHTUSDT")),
        windows_seconds=parse_windows_minutes(payload.get("windows_minutes", [5, 10])),
        report_interval=report_interval,
        baseline_minutes=baseline_minutes,
        runtime_seconds=parse_runtime(payload.get("runtime_seconds", 0)),
    )


def build_multi_symbol_overview(sessions: list[dict[str, object]]) -> dict[str, object]:
    populated = [session for session in sessions if session.get("snapshot")]
    if not populated:
        return {
            "headline": "等待启动",
            "summary": "启动监控后，这里会汇总当前最强的币种信号。",
            "level": "warming",
            "strongest_symbol": None,
            "strongest_window": None,
            "symbol_count": len(sessions),
            "ready_count": 0,
            "phase_text": "等待启动",
        }

    priority = {"high": 4, "watch": 3, "active": 2, "low": 1, "warming": 0}
    strongest = max(
        populated,
        key=lambda item: (
            priority[item["snapshot"]["overall"]["level"]],
            item["snapshot"]["overall"]["window_label"] or "",
        ),
    )
    strongest_snapshot = strongest["snapshot"]
    strongest_overall = strongest_snapshot["overall"]

    return {
        "headline": strongest_overall["headline"],
        "summary": f"当前最强信号来自 {strongest['symbol']}，重点看 {strongest_overall['window_label']} 窗口。",
        "level": strongest_overall["level"],
        "strongest_symbol": strongest["symbol"],
        "strongest_window": strongest_overall["window_label"],
        "symbol_count": len(sessions),
        "ready_count": len(populated),
        "phase_text": f"已就绪 {len(populated)}/{len(sessions)} 个币种",
    }


class MonitorManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._logs: deque[str] = deque(maxlen=180)
        self._config = SessionConfig()
        self._mode = "idle"
        self._message = "等待启动"
        self._last_error: Optional[str] = None
        self._snapshots: dict[str, dict[str, object]] = {}
        self._started_at = 0.0
        self._stopped_at = 0.0
        self._last_update_at = time.time()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._monitors: dict[str, object] = {}
        self._symbol_tasks: dict[str, list[asyncio.Task[object]]] = {}
        self._runtime_task: Optional[asyncio.Task[object]] = None

    def state(self) -> dict[str, object]:
        with self._lock:
            sessions = [
                {
                    "symbol": symbol,
                    "snapshot": self._snapshots.get(symbol),
                }
                for symbol in self._config.symbols
            ]
            return {
                "mode": self._mode,
                "message": self._message,
                "last_error": self._last_error,
                "config": self._config.to_dict(),
                "snapshot": self._snapshots.get(self._config.symbols[0]) if self._config.symbols else None,
                "snapshots": self._snapshots,
                "sessions": sessions,
                "overview": build_multi_symbol_overview(sessions),
                "logs": list(self._logs),
                "started_at": self._started_at or None,
                "started_at_text": format_clock(self._started_at) if self._started_at else None,
                "stopped_at": self._stopped_at or None,
                "stopped_at_text": format_clock(self._stopped_at) if self._stopped_at else None,
                "last_update_at": self._last_update_at,
                "last_update_at_text": format_clock(self._last_update_at),
                "is_active": self._mode in {"starting", "running", "stopping"},
                "can_add_symbols": self._mode in {"starting", "running"},
                "can_manage_symbols": self._mode in {"starting", "running"},
                "app_version": APP_VERSION,
            }

    def restart(self, config: SessionConfig) -> None:
        self.stop(wait=True, timeout=10.0)
        with self._lock:
            self._config = config
            self._mode = "starting"
            self._message = f"正在启动 {len(config.symbols)} 个币种"
            self._last_error = None
            self._snapshots = {}
            self._logs.clear()
            self._started_at = 0.0
            self._stopped_at = 0.0
            self._last_update_at = time.time()
            thread = threading.Thread(target=self._run_thread, args=(config,), daemon=True)
            self._thread = thread
        self._append_log(f"[{format_clock()}] session=start symbols={','.join(config.symbols)}")
        thread.start()

    def stop(self, wait: bool = False, timeout: float = 10.0) -> bool:
        with self._lock:
            thread = self._thread
            loop = self._loop
            stop_event = self._stop_event
            active = bool(thread and thread.is_alive())
            if active and self._mode != "stopping":
                self._mode = "stopping"
                self._message = "正在停止监控"
                self._last_update_at = time.time()
        if active and loop and stop_event:
            loop.call_soon_threadsafe(stop_event.set)
        if wait and thread:
            thread.join(timeout=timeout)
        return active

    def shutdown(self) -> None:
        self.stop(wait=True, timeout=10.0)

    def add_symbols(self, symbols: list[str], timeout: float = 60.0) -> dict[str, object]:
        with self._lock:
            loop = self._loop
            active = self._mode in {"starting", "running"}
            current_symbols = list(self._config.symbols)
        if not active or loop is None:
            raise RuntimeError("请先启动监控，再增量添加币种")

        unique_symbols = []
        seen = set()
        for symbol in symbols:
            upper = symbol.upper()
            if upper not in seen:
                seen.add(upper)
                unique_symbols.append(upper)

        missing = [symbol for symbol in unique_symbols if symbol not in current_symbols]
        existing = [symbol for symbol in unique_symbols if symbol in current_symbols]
        if not missing:
            return {"added": [], "existing": existing, "failed": []}

        future = asyncio.run_coroutine_threadsafe(self._add_symbols_async(missing), loop)
        result = future.result(timeout=timeout)
        result["existing"] = existing
        return result

    def remove_symbols(self, symbols: list[str], timeout: float = 60.0) -> dict[str, object]:
        with self._lock:
            loop = self._loop
            active = self._mode in {"starting", "running"}
            current_symbols = list(self._config.symbols)
        if not active or loop is None:
            raise RuntimeError("请先启动监控，再移除币种")

        unique_symbols = []
        seen = set()
        for symbol in symbols:
            upper = symbol.upper()
            if upper not in seen:
                seen.add(upper)
                unique_symbols.append(upper)

        removable = [symbol for symbol in unique_symbols if symbol in current_symbols]
        missing = [symbol for symbol in unique_symbols if symbol not in current_symbols]
        if not removable:
            return {"removed": [], "missing": missing}

        future = asyncio.run_coroutine_threadsafe(self._remove_symbols_async(removable), loop)
        result = future.result(timeout=timeout)
        result["missing"] = missing
        return result

    def sync_symbols(self, symbols: list[str], timeout: float = 60.0) -> dict[str, object]:
        with self._lock:
            loop = self._loop
            active = self._mode in {"starting", "running"}
            current_symbols = list(self._config.symbols)
        target_symbols = []
        seen = set()
        for symbol in symbols:
            upper = symbol.upper()
            if upper not in seen:
                seen.add(upper)
                target_symbols.append(upper)

        if not active or loop is None:
            config = SessionConfig(
                symbols=target_symbols,
                windows_seconds=list(self._config.windows_seconds),
                report_interval=self._config.report_interval,
                baseline_minutes=self._config.baseline_minutes,
                runtime_seconds=self._config.runtime_seconds,
                thresholds=self._config.thresholds,
            )
            self.restart(config)
            return {
                "mode": "restart",
                "added": target_symbols,
                "removed": [],
                "symbols": target_symbols,
            }

        current_set = set(current_symbols)
        target_set = set(target_symbols)
        to_add = [symbol for symbol in target_symbols if symbol not in current_set]
        to_remove = [symbol for symbol in current_symbols if symbol not in target_set]

        future = asyncio.run_coroutine_threadsafe(self._sync_symbols_async(target_symbols, to_add, to_remove), loop)
        return future.result(timeout=timeout)

    def _run_thread(self, config: SessionConfig) -> None:
        try:
            asyncio.run(self._async_run(config))
        except Exception as exc:
            self._set_error(f"{exc.__class__.__name__}: {exc}", traceback.format_exc())

    async def _async_run(self, config: SessionConfig) -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        self._bind_runtime(loop, stop_event)

        final_mode = "stopped"
        final_message = f"{len(config.symbols)} 个币种已停止"

        try:
            for symbol in config.symbols:
                await self._start_symbol_async(symbol, config)

            self._set_running(config)
            if config.runtime_seconds > 0:
                runtime_task = asyncio.create_task(runtime_guard(config.runtime_seconds, stop_event))
                with self._lock:
                    self._runtime_task = runtime_task

            await stop_event.wait()
        except Exception as exc:
            final_mode = "error"
            final_message = f"{len(config.symbols)} 个币种启动失败"
            self._set_error(str(exc), traceback.format_exc())
        finally:
            await self._cancel_all_tasks()
            self._unbind_runtime()
            if final_mode != "error":
                self._publish_mode(final_mode, final_message)

    async def _snapshot_loop(self, symbol: str, monitor, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            self._publish_symbol_snapshot(symbol, monitor.snapshot())
            await asyncio.sleep(1.0)

    def _bind_runtime(self, loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event) -> None:
        with self._lock:
            self._loop = loop
            self._stop_event = stop_event

    def _unbind_runtime(self) -> None:
        with self._lock:
            self._loop = None
            self._stop_event = None
            self._thread = None
            self._monitors = {}
            self._symbol_tasks = {}
            self._runtime_task = None

    def _set_running(self, config: SessionConfig) -> None:
        with self._lock:
            self._mode = "running"
            self._message = f"正在监控 {len(config.symbols)} 个币种"
            self._started_at = time.time()
            self._stopped_at = 0.0
            self._last_update_at = self._started_at

    def _publish_symbol_snapshot(self, symbol: str, snapshot: dict[str, object]) -> None:
        with self._lock:
            self._snapshots[symbol] = snapshot
            self._last_update_at = time.time()

    def _publish_mode(self, mode: str, message: str) -> None:
        with self._lock:
            self._mode = mode
            self._message = message
            self._stopped_at = time.time()
            self._last_update_at = self._stopped_at
        self._append_log(f"[{format_clock()}] session={mode}")

    def _set_error(self, message: str, details: str) -> None:
        with self._lock:
            self._mode = "error"
            self._message = "监控异常"
            self._last_error = message
            self._stopped_at = time.time()
            self._last_update_at = self._stopped_at
        self._append_log(f"[{format_clock()}] error={message}")
        self._append_log(details.strip())

    def _append_log(self, message: str) -> None:
        with self._lock:
            self._logs.append(message)
            self._last_update_at = time.time()

    async def _start_symbol_async(self, symbol: str, config: SessionConfig) -> None:
        monitor = await asyncio.to_thread(
            create_monitor,
            symbol,
            config.windows_seconds,
            config.thresholds,
            config.baseline_minutes,
            self._append_log,
        )
        self._publish_symbol_snapshot(symbol, monitor.snapshot())
        self._append_log(monitor.render_report())
        with self._lock:
            self._monitors[symbol] = monitor
            stop_event = self._stop_event
        assert stop_event is not None
        tasks = [
            asyncio.create_task(stream_loop(symbol, monitor, stop_event, logger=self._append_log)),
            asyncio.create_task(report_loop(monitor, config.report_interval, logger=self._append_log)),
            asyncio.create_task(self._snapshot_loop(symbol, monitor, stop_event)),
        ]
        with self._lock:
            self._symbol_tasks[symbol] = tasks

    async def _add_symbols_async(self, symbols: list[str]) -> dict[str, object]:
        added: list[str] = []
        failed: list[dict[str, str]] = []
        for symbol in symbols:
            with self._lock:
                if symbol in self._monitors:
                    continue
                config = SessionConfig(
                    symbols=list(self._config.symbols),
                    windows_seconds=list(self._config.windows_seconds),
                    report_interval=self._config.report_interval,
                    baseline_minutes=self._config.baseline_minutes,
                    runtime_seconds=self._config.runtime_seconds,
                    thresholds=self._config.thresholds,
                )
            try:
                await self._start_symbol_async(symbol, config)
                with self._lock:
                    if symbol not in self._config.symbols:
                        self._config.symbols.append(symbol)
                        self._message = f"正在监控 {len(self._config.symbols)} 个币种"
                        self._last_update_at = time.time()
                added.append(symbol)
                self._append_log(f"[{format_clock()}] symbol=added {symbol}")
            except Exception as exc:
                failed.append({"symbol": symbol, "error": str(exc)})
                self._append_log(f"[{format_clock()}] symbol=add_failed {symbol}: {exc}")
        return {"added": added, "failed": failed}

    async def _remove_symbols_async(self, symbols: list[str]) -> dict[str, object]:
        removed: list[str] = []
        for symbol in symbols:
            with self._lock:
                tasks = list(self._symbol_tasks.get(symbol, []))
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            with self._lock:
                self._symbol_tasks.pop(symbol, None)
                self._monitors.pop(symbol, None)
                self._snapshots.pop(symbol, None)
                if symbol in self._config.symbols:
                    self._config.symbols.remove(symbol)
                    removed.append(symbol)
                count = len(self._config.symbols)
                if self._mode in {"starting", "running"}:
                    self._message = f"正在监控 {count} 个币种" if count else "当前没有币种，等待添加"
                    self._last_update_at = time.time()
            if symbol in removed:
                self._append_log(f"[{format_clock()}] symbol=removed {symbol}")
        return {"removed": removed}

    async def _sync_symbols_async(
        self,
        target_symbols: list[str],
        to_add: list[str],
        to_remove: list[str],
    ) -> dict[str, object]:
        removed_result = await self._remove_symbols_async(to_remove) if to_remove else {"removed": []}
        added_result = await self._add_symbols_async(to_add) if to_add else {"added": [], "failed": []}
        with self._lock:
            self._config.symbols = list(target_symbols)
            count = len(self._config.symbols)
            self._message = f"正在监控 {count} 个币种" if count else "当前没有币种，等待添加"
            self._last_update_at = time.time()
        return {
            "mode": "sync",
            "added": added_result.get("added", []),
            "failed": added_result.get("failed", []),
            "removed": removed_result.get("removed", []),
            "symbols": list(target_symbols),
        }

    async def _cancel_all_tasks(self) -> None:
        with self._lock:
            tasks = [task for group in self._symbol_tasks.values() for task in group]
            runtime_task = self._runtime_task
        if runtime_task is not None:
            tasks.append(runtime_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


manager = MonitorManager()
app = Flask(__name__, static_folder=str(UI_DIR), static_url_path="/ui")
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/")
def index():
    return send_from_directory(UI_DIR, "index.html")


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
        symbols = parse_symbols(payload.get("symbols", ""))
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
    if not args.no_browser:
        threading.Timer(1.2, lambda: webbrowser.open(url)).start()
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
    return 0


atexit.register(clear_pid_file)
atexit.register(manager.shutdown)


if __name__ == "__main__":
    raise SystemExit(main())
