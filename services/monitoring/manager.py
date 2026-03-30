from __future__ import annotations

import asyncio
import threading
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from app_config import APP_VERSION
from monitor import create_monitor, format_clock, report_loop, runtime_guard, stream_loop
from services.monitoring.config import SessionConfig, parse_symbols
from services.monitoring.overview import build_multi_symbol_overview, build_sessions


@dataclass(slots=True)
class RuntimeHandles:
    thread: Optional[threading.Thread] = None
    loop: Optional[asyncio.AbstractEventLoop] = None
    stop_event: Optional[asyncio.Event] = None
    monitors: dict[str, object] = field(default_factory=dict)
    symbol_tasks: dict[str, list[asyncio.Task[object]]] = field(default_factory=dict)
    runtime_task: Optional[asyncio.Task[object]] = None


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
        self._runtime = RuntimeHandles()

    def state(self) -> dict[str, object]:
        with self._lock:
            sessions = build_sessions(self._config.symbols, self._snapshots)
            primary_symbol = self._config.symbols[0] if self._config.symbols else None
            return {
                "mode": self._mode,
                "message": self._message,
                "last_error": self._last_error,
                "config": self._config.to_dict(),
                "snapshot": self._snapshots.get(primary_symbol) if primary_symbol else None,
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
        thread = threading.Thread(target=self._run_thread, args=(config,), daemon=True)
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
            self._runtime.thread = thread
        self._append_log(f"[{format_clock()}] session=start symbols={','.join(config.symbols)}")
        thread.start()

    def stop(self, wait: bool = False, timeout: float = 10.0) -> bool:
        with self._lock:
            thread = self._runtime.thread
            loop = self._runtime.loop
            stop_event = self._runtime.stop_event
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
            active = self._mode in {"starting", "running"}
            current_symbols = list(self._config.symbols)
        if not active or self._runtime.loop is None:
            raise RuntimeError("请先启动监控，再增量添加币种")

        unique_symbols = parse_symbols(symbols, allow_empty=True)
        missing = [symbol for symbol in unique_symbols if symbol not in current_symbols]
        existing = [symbol for symbol in unique_symbols if symbol in current_symbols]
        if not missing:
            return {"added": [], "existing": existing, "failed": []}

        result = self._submit_runtime_task(self._add_symbols_async(missing), timeout)
        result["existing"] = existing
        return result

    def remove_symbols(self, symbols: list[str], timeout: float = 60.0) -> dict[str, object]:
        with self._lock:
            active = self._mode in {"starting", "running"}
            current_symbols = list(self._config.symbols)
        if not active or self._runtime.loop is None:
            raise RuntimeError("请先启动监控，再移除币种")

        unique_symbols = parse_symbols(symbols, allow_empty=True)
        removable = [symbol for symbol in unique_symbols if symbol in current_symbols]
        missing = [symbol for symbol in unique_symbols if symbol not in current_symbols]
        if not removable:
            return {"removed": [], "missing": missing}

        result = self._submit_runtime_task(self._remove_symbols_async(removable), timeout)
        result["missing"] = missing
        return result

    def sync_symbols(self, symbols: list[str], timeout: float = 60.0) -> dict[str, object]:
        target_symbols = parse_symbols(symbols, allow_empty=True)
        with self._lock:
            active = self._mode in {"starting", "running"}
            current_symbols = list(self._config.symbols)
            loop = self._runtime.loop

        if not active or loop is None:
            added = [symbol for symbol in target_symbols if symbol not in current_symbols]
            removed = [symbol for symbol in current_symbols if symbol not in target_symbols]
            with self._lock:
                self._config = self._config.clone(symbols=target_symbols)
                self._last_update_at = time.time()
                self._message = "等待启动" if not target_symbols else f"已更新待启动币种（{len(target_symbols)} 个）"
            return {
                "mode": "staged",
                "added": added,
                "removed": removed,
                "symbols": target_symbols,
            }

        current_set = set(current_symbols)
        target_set = set(target_symbols)
        to_add = [symbol for symbol in target_symbols if symbol not in current_set]
        to_remove = [symbol for symbol in current_symbols if symbol not in target_set]
        return self._submit_runtime_task(self._sync_symbols_async(target_symbols, to_add, to_remove), timeout)

    def _submit_runtime_task(self, coro, timeout: float) -> dict[str, object]:
        loop = self._runtime.loop
        if loop is None:
            raise RuntimeError("监控运行时未启动")
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        result = future.result(timeout=timeout)
        assert isinstance(result, dict)
        return result

    def _run_thread(self, config: SessionConfig) -> None:
        try:
            asyncio.run(self._async_run(config))
        except Exception as exc:
            self._set_error(f"{exc.__class__.__name__}: {exc}", traceback.format_exc())

    async def _async_run(self, config: SessionConfig) -> None:
        stop_event = asyncio.Event()
        self._bind_runtime(asyncio.get_running_loop(), stop_event)

        final_mode = "stopped"
        final_message = f"{len(config.symbols)} 个币种已停止"
        try:
            for symbol in config.symbols:
                await self._start_symbol_async(symbol, config)

            self._set_running(config)
            if config.runtime_seconds > 0:
                runtime_task = asyncio.create_task(runtime_guard(config.runtime_seconds, stop_event))
                with self._lock:
                    self._runtime.runtime_task = runtime_task

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
            self._runtime.loop = loop
            self._runtime.stop_event = stop_event

    def _unbind_runtime(self) -> None:
        with self._lock:
            self._runtime = RuntimeHandles()

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
            self._runtime.monitors[symbol] = monitor
            stop_event = self._runtime.stop_event
        assert stop_event is not None
        tasks = [
            asyncio.create_task(stream_loop(symbol, monitor, stop_event, logger=self._append_log)),
            asyncio.create_task(report_loop(monitor, config.report_interval, logger=self._append_log)),
            asyncio.create_task(self._snapshot_loop(symbol, monitor, stop_event)),
        ]
        with self._lock:
            self._runtime.symbol_tasks[symbol] = tasks

    async def _add_symbols_async(self, symbols: list[str]) -> dict[str, object]:
        added: list[str] = []
        failed: list[dict[str, str]] = []
        for symbol in symbols:
            with self._lock:
                if symbol in self._runtime.monitors:
                    continue
                config = self._config.clone()
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
                tasks = list(self._runtime.symbol_tasks.get(symbol, []))
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            with self._lock:
                self._runtime.symbol_tasks.pop(symbol, None)
                self._runtime.monitors.pop(symbol, None)
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
            tasks = [task for group in self._runtime.symbol_tasks.values() for task in group]
            if self._runtime.runtime_task is not None:
                tasks.append(self._runtime.runtime_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
