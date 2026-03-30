from __future__ import annotations

import http.client
import json
import os
import shutil
import subprocess
import threading
import time
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from app_config import DATA_DIR

OPENCLI_CACHE_TTL_SECONDS = 180
OPENCLI_FETCH_WORKERS = 4
OPENCLI_STALE_CACHE_TTL_SECONDS = 15 * 60
OPENCLI_STATUS_CACHE_TTL_SECONDS = 8
OPENCLI_RECENT_SUCCESS_TTL_SECONDS = 10 * 60
OPENCLI_COMMAND_RETRY_ATTEMPTS = 2
OPENCLI_COMMAND_RETRY_BACKOFF_SECONDS = 1.0
OPENCLI_AUTO_CLOSE_DELAY_SECONDS = float(os.environ.get("OPENCLI_AUTO_CLOSE_DELAY_SECONDS", "8"))
OPENCLI_SELF_HEAL_ENABLED = str(os.environ.get("OPENCLI_SELF_HEAL_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}
OPENCLI_SELF_HEAL_COOLDOWN_SECONDS = float(os.environ.get("OPENCLI_SELF_HEAL_COOLDOWN_SECONDS", "20"))
OPENCLI_SELF_HEAL_EXTENSION_WAIT_SECONDS = float(os.environ.get("OPENCLI_SELF_HEAL_EXTENSION_WAIT_SECONDS", "5"))

_OPENCLI_JSON_CACHE: dict[str, tuple[float, list[dict[str, object]]]] = {}
_OPENCLI_CACHE_LOCK = threading.RLock()
_OPENCLI_BROWSER_LOCK = threading.Lock()
_OPENCLI_CLEANUP_LOCK = threading.RLock()
_OPENCLI_SELF_HEAL_LOCK = threading.Lock()
_OPENCLI_STATUS_CACHE: tuple[float, dict[str, object]] | None = None
_OPENCLI_LAST_SUCCESS: dict[str, object] = {
    "at": 0.0,
    "args": [],
}
_OPENCLI_CLEANUP_TIMERS: dict[str, threading.Timer] = {}
_OPENCLI_CLEANUP_GENERATIONS: dict[str, int] = {}
_OPENCLI_LAST_SELF_HEAL: dict[str, object] = {
    "at": 0.0,
    "reason": "",
}


class OpencliBusyError(RuntimeError):
    pass


def find_opencli_bin() -> str | None:
    candidates = [
        shutil.which("opencli"),
        "/opt/homebrew/bin/opencli",
        "/usr/local/bin/opencli",
    ]
    for candidate in candidates:
        if candidate and os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def _command_uses_browser(args: list[str]) -> bool:
    if not args:
        return False
    if args[0] == "doctor":
        return True
    if args[0] == "twitter":
        return True
    return False


def _command_counts_as_success(args: list[str]) -> bool:
    if not args:
        return False
    if args[0] == "--version":
        return False
    if args[0] == "doctor":
        return False
    return _command_uses_browser(args)


def _infer_opencli_workspace(args: list[str]) -> str | None:
    if not args:
        return None
    head = str(args[0]).strip().lower()
    if not head:
        return None
    if head == "doctor":
        return "default"
    if head in {
        "--version",
        "help",
        "list",
        "validate",
        "verify",
        "explore",
        "probe",
        "synthesize",
        "generate",
        "record",
        "cascade",
        "completion",
        "plugin",
        "install",
        "register",
    }:
        return None
    return f"site:{head}"


def _cancel_opencli_cleanup(workspace: str) -> None:
    with _OPENCLI_CLEANUP_LOCK:
        timer = _OPENCLI_CLEANUP_TIMERS.pop(workspace, None)
        _OPENCLI_CLEANUP_GENERATIONS.pop(workspace, None)
    if timer:
        timer.cancel()


def _opencli_daemon_command(
    action: str,
    payload: dict[str, object],
    *,
    timeout: float = 8.0,
) -> dict[str, object]:
    port = int(os.environ.get("OPENCLI_DAEMON_PORT", "19825"))
    body = {
        "id": f"intel_{action}_{time.time_ns()}",
        "action": action,
        **payload,
    }
    request = urllib_request.Request(
        f"http://127.0.0.1:{port}/command",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-OpenCLI": "1",
        },
        method="POST",
    )
    with urllib_request.urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    data = json.loads(raw) if raw.strip() else {}
    if not isinstance(data, dict):
        raise RuntimeError("opencli daemon 返回了非对象结果")
    if not data.get("ok"):
        raise RuntimeError(str(data.get("error") or "opencli daemon 执行失败"))
    return data


def _read_opencli_daemon_status(*, timeout: float = 2.0) -> dict[str, object] | None:
    port = int(os.environ.get("OPENCLI_DAEMON_PORT", "19825"))
    request = urllib_request.Request(
        f"http://127.0.0.1:{port}/status",
        headers={"X-OpenCLI": "1"},
        method="GET",
    )
    try:
        with urllib_request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, ValueError):
        return None
    try:
        data = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _is_opencli_extension_connected(*, timeout: float = 1.0) -> bool:
    status = _read_opencli_daemon_status(timeout=timeout)
    return bool(status and status.get("extensionConnected"))


def _run_osascript(lines: list[str], *, timeout: float = 12.0) -> subprocess.CompletedProcess[str]:
    osascript_bin = shutil.which("osascript") or "/usr/bin/osascript"
    command = [osascript_bin]
    for line in lines:
        command.extend(["-e", line])
    return subprocess.run(
        command,
        cwd=str(DATA_DIR.parent),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def _chrome_window_count() -> int | None:
    proc = _run_osascript(
        [
            'if application "Google Chrome" is running then',
            '  tell application "Google Chrome" to return count of windows',
            "else",
            "  return 0",
            "end if",
        ],
        timeout=6.0,
    )
    if proc.returncode != 0:
        return None
    try:
        return max(int((proc.stdout or "0").strip() or "0"), 0)
    except Exception:
        return None


def _ensure_chrome_window_for_opencli() -> bool:
    proc = _run_osascript(
        [
            'if application "Google Chrome" is running then',
            '  tell application "Google Chrome"',
            "    if (count of windows) is 0 then make new window",
            "    return count of windows",
            "  end tell",
            "else",
            '  tell application "Google Chrome" to launch',
            "  delay 0.8",
            '  tell application "Google Chrome"',
            "    if (count of windows) is 0 then make new window",
            "    return count of windows",
            "  end tell",
            "end if",
        ],
        timeout=15.0,
    )
    if proc.returncode != 0:
        return False
    try:
        return int((proc.stdout or "0").strip() or "0") > 0
    except Exception:
        return False


def _maybe_self_heal_opencli_browser(reason: str) -> bool:
    if not OPENCLI_SELF_HEAL_ENABLED:
        return False
    if _is_opencli_extension_connected(timeout=0.8):
        return False

    with _OPENCLI_SELF_HEAL_LOCK:
        if _is_opencli_extension_connected(timeout=0.8):
            return False

        now_value = time.time()
        last_at = _OPENCLI_LAST_SELF_HEAL.get("at")
        if isinstance(last_at, (int, float)) and now_value - float(last_at) < OPENCLI_SELF_HEAL_COOLDOWN_SECONDS:
            return False

        window_count = _chrome_window_count()
        if window_count is None or window_count > 0:
            return False

        created = _ensure_chrome_window_for_opencli()
        if not created:
            return False

        _OPENCLI_LAST_SELF_HEAL["at"] = time.time()
        _OPENCLI_LAST_SELF_HEAL["reason"] = reason

    daemon_status = _read_opencli_daemon_status(timeout=0.8)
    if daemon_status and daemon_status.get("ok"):
        deadline = time.time() + max(OPENCLI_SELF_HEAL_EXTENSION_WAIT_SECONDS, 0.0)
        while time.time() < deadline:
            if _is_opencli_extension_connected(timeout=0.8):
                return True
            time.sleep(0.4)
    return True


def _close_opencli_workspace_window(workspace: str) -> None:
    try:
        _opencli_daemon_command("close-window", {"workspace": workspace})
    except (
        urllib_error.URLError,
        urllib_error.HTTPError,
        http.client.HTTPException,
        OSError,
        TimeoutError,
        ValueError,
        RuntimeError,
    ):
        return


def _run_scheduled_opencli_cleanup(workspace: str, generation: int) -> None:
    _OPENCLI_BROWSER_LOCK.acquire()
    try:
        with _OPENCLI_CLEANUP_LOCK:
            current_generation = _OPENCLI_CLEANUP_GENERATIONS.get(workspace)
            if current_generation != generation:
                return
        _close_opencli_workspace_window(workspace)
        with _OPENCLI_CLEANUP_LOCK:
            if _OPENCLI_CLEANUP_GENERATIONS.get(workspace) == generation:
                _OPENCLI_CLEANUP_GENERATIONS.pop(workspace, None)
                _OPENCLI_CLEANUP_TIMERS.pop(workspace, None)
    finally:
        _OPENCLI_BROWSER_LOCK.release()


def _schedule_opencli_cleanup(workspace: str) -> None:
    if not workspace or OPENCLI_AUTO_CLOSE_DELAY_SECONDS < 0:
        return
    with _OPENCLI_CLEANUP_LOCK:
        previous = _OPENCLI_CLEANUP_TIMERS.pop(workspace, None)
        generation = _OPENCLI_CLEANUP_GENERATIONS.get(workspace, 0) + 1
        _OPENCLI_CLEANUP_GENERATIONS[workspace] = generation
        timer = threading.Timer(
            max(OPENCLI_AUTO_CLOSE_DELAY_SECONDS, 0.0),
            _run_scheduled_opencli_cleanup,
            args=(workspace, generation),
        )
        timer.daemon = True
        _OPENCLI_CLEANUP_TIMERS[workspace] = timer
    if previous:
        previous.cancel()
    timer.start()


def _set_status_cache(payload: dict[str, object]) -> dict[str, object]:
    global _OPENCLI_STATUS_CACHE
    cached = dict(payload)
    _OPENCLI_STATUS_CACHE = (time.time(), cached)
    return dict(cached)


def _get_status_cache(*, fresh_only: bool = True) -> dict[str, object] | None:
    cached = _OPENCLI_STATUS_CACHE
    if not cached:
        return None
    cached_at, payload = cached
    if fresh_only and time.time() - cached_at > OPENCLI_STATUS_CACHE_TTL_SECONDS:
        return None
    return dict(payload)


def _get_status_cache_state() -> tuple[dict[str, object] | None, int | None, bool]:
    cached = _OPENCLI_STATUS_CACHE
    if not cached:
        return None, None, False
    cached_at, payload = cached
    age_seconds = max(int(time.time() - float(cached_at)), 0)
    return dict(payload), age_seconds, age_seconds > OPENCLI_STATUS_CACHE_TTL_SECONDS


def _record_opencli_success(args: list[str]) -> None:
    global _OPENCLI_LAST_SUCCESS
    now_value = time.time()
    _OPENCLI_LAST_SUCCESS = {
        "at": now_value,
        "args": list(args),
    }
    cached = _get_status_cache(fresh_only=False)
    if cached:
        cached["connected"] = True
        cached["message"] = "最近抓取成功"
        cached["hint"] = None
        cached["last_success_at"] = now_value
        cached["last_success_command"] = " ".join(args)
        _set_status_cache(cached)


def _recent_opencli_success() -> dict[str, object] | None:
    at_value = _OPENCLI_LAST_SUCCESS.get("at")
    if not isinstance(at_value, (int, float)) or at_value <= 0:
        return None
    if time.time() - float(at_value) > OPENCLI_RECENT_SUCCESS_TTL_SECONDS:
        return None
    args = _OPENCLI_LAST_SUCCESS.get("args")
    if not isinstance(args, list):
        args = []
    return {
        "at": float(at_value),
        "args": [str(arg) for arg in args],
    }


def get_opencli_runtime_status(*, timeout: float = 0.6) -> dict[str, object]:
    opencli_bin = find_opencli_bin()
    if not opencli_bin:
        return {
            "ok": True,
            "installed": False,
            "connected": False,
            "status_stale": False,
            "status_age_seconds": None,
            "connection_source": "missing",
            "message": "未安装 opencli",
            "hint": "先执行 npm install -g @jackwener/opencli",
        }

    cached, status_age_seconds, status_stale = _get_status_cache_state()
    recent_success = _recent_opencli_success()
    recent_success_age_seconds = None
    if recent_success:
        recent_success_age_seconds = max(int(time.time() - float(recent_success.get("at") or 0.0)), 0)
    daemon_status = _read_opencli_daemon_status(timeout=timeout)
    chrome_window_count = _chrome_window_count()
    auto_recover_on_demand = bool(
        OPENCLI_SELF_HEAL_ENABLED
        and isinstance(chrome_window_count, int)
        and chrome_window_count == 0
    )

    connected = False
    connection_source = ""
    if daemon_status and daemon_status.get("extensionConnected"):
        connected = True
        connection_source = "daemon"
    elif isinstance(cached, dict) and bool(cached.get("connected")):
        connected = True
        connection_source = str(cached.get("connection_source") or "cache")
    elif recent_success:
        connected = True
        connection_source = "recent_success"

    message = ""
    hint = None
    if isinstance(cached, dict):
        message = str(cached.get("message") or "")
        cached_hint = cached.get("hint")
        if cached_hint is not None:
            hint = str(cached_hint)
    if not message:
        if connected:
            message = "已连通" if connection_source == "daemon" else "最近抓取成功"
        elif auto_recover_on_demand:
            message = "待机中，抓取时自动恢复"
        else:
            message = "未连通浏览器扩展"
    if hint is None and not connected:
        if auto_recover_on_demand:
            hint = "当前未打开 Chrome 窗口；执行抓取时会自动拉起最小窗口，无需手动处理。"
        else:
            hint = "请在 Chrome 安装并启用 opencli Browser Bridge 扩展，然后重试。"

    payload: dict[str, Any] = {
        "ok": True,
        "installed": True,
        "connected": connected,
        "binary": opencli_bin,
        "message": message,
        "hint": hint,
        "connection_source": connection_source or "unknown",
        "auto_recover_on_demand": auto_recover_on_demand,
        "chrome_window_count": chrome_window_count,
        "status_stale": status_stale,
        "status_age_seconds": status_age_seconds,
        "last_success_at": recent_success.get("at") if recent_success else None,
        "last_success_age_seconds": recent_success_age_seconds,
        "last_success_command": " ".join(recent_success.get("args") or []) if recent_success else "",
        "daemon_ok": bool(daemon_status and daemon_status.get("ok")),
        "daemon_extension_connected": bool(daemon_status and daemon_status.get("extensionConnected")),
    }
    if isinstance(cached, dict):
        if cached.get("version"):
            payload["version"] = cached.get("version")
        if cached.get("last_status_error"):
            payload["last_status_error"] = cached.get("last_status_error")
    return payload


def run_opencli_command(
    args: list[str],
    timeout: int = 45,
    *,
    browser_lock_timeout: float | None = None,
    allow_self_heal: bool = True,
) -> subprocess.CompletedProcess[str]:
    opencli_bin = find_opencli_bin()
    if not opencli_bin:
        raise RuntimeError("未检测到 opencli，请先安装：npm install -g @jackwener/opencli")

    env = dict(os.environ)
    path_parts = [
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
    ]
    current_path = env.get("PATH", "")
    merged = ":".join([*path_parts, current_path]) if current_path else ":".join(path_parts)
    env["PATH"] = merged

    uses_browser = _command_uses_browser(args)
    workspace = _infer_opencli_workspace(args) if uses_browser else None
    lock_acquired = False
    try:
        if uses_browser:
            if workspace:
                _cancel_opencli_cleanup(workspace)
            if browser_lock_timeout is None:
                _OPENCLI_BROWSER_LOCK.acquire()
                lock_acquired = True
            else:
                lock_acquired = _OPENCLI_BROWSER_LOCK.acquire(timeout=max(browser_lock_timeout, 0.0))
                if not lock_acquired:
                    raise OpencliBusyError("opencli 浏览器正在忙，请稍后重试")
            if allow_self_heal:
                _maybe_self_heal_opencli_browser("command")

        proc = subprocess.run(
            [opencli_bin, *args],
            cwd=str(DATA_DIR.parent),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
            env=env,
        )
        if proc.returncode == 0 and _command_counts_as_success(args):
            _record_opencli_success(args)
        return proc
    finally:
        if lock_acquired:
            _OPENCLI_BROWSER_LOCK.release()
        if lock_acquired and workspace:
            _schedule_opencli_cleanup(workspace)


def parse_json_output(stdout: str) -> object:
    text = stdout.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        first_obj = text.find("{")
        first_arr = text.find("[")
        starts = [index for index in (first_obj, first_arr) if index >= 0]
        if starts:
            start = min(starts)
            fragment = text[start:]
            try:
                return json.loads(fragment)
            except json.JSONDecodeError:
                pass
        raise ValueError("opencli 返回了非 JSON 输出，请检查命令参数或扩展连接状态")


def run_opencli_json_command(
    args: list[str],
    timeout: int = 90,
    *,
    retries: int = OPENCLI_COMMAND_RETRY_ATTEMPTS,
    retry_backoff_seconds: float = OPENCLI_COMMAND_RETRY_BACKOFF_SECONDS,
    browser_lock_timeout: float | None = None,
) -> tuple[object, subprocess.CompletedProcess[str]]:
    attempts = max(int(retries), 1)
    last_error = "unknown error"
    last_proc: subprocess.CompletedProcess[str] | None = None
    for attempt in range(attempts):
        proc = run_opencli_command(args, timeout=timeout, browser_lock_timeout=browser_lock_timeout)
        last_proc = proc
        if proc.returncode == 0:
            try:
                return parse_json_output(proc.stdout), proc
            except Exception as exc:
                last_error = str(exc) or "opencli 返回了非 JSON 输出"
        else:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            last_error = stderr or stdout or f"opencli 返回退出码 {proc.returncode}"

        if attempt + 1 < attempts:
            time.sleep(retry_backoff_seconds * (attempt + 1))

    raise RuntimeError(last_error or "opencli 执行失败") from None


def fetch_opencli_json(
    args: list[str],
    timeout: int = 90,
    *,
    cache_ttl_seconds: int = OPENCLI_CACHE_TTL_SECONDS,
) -> list[dict[str, object]]:
    cache_key = json.dumps(args, ensure_ascii=False)
    now_value = time.time()
    with _OPENCLI_CACHE_LOCK:
        cached = _OPENCLI_JSON_CACHE.get(cache_key)
    if cached:
        cached_at, rows = cached
        if now_value - cached_at <= cache_ttl_seconds:
            return [dict(row) for row in rows]

    try:
        parsed, _ = run_opencli_json_command(args, timeout=timeout)
    except Exception:
        if cached:
            cached_at, rows = cached
            if now_value - cached_at <= OPENCLI_STALE_CACHE_TTL_SECONDS:
                return [dict(row) for row in rows]
        return []

    if isinstance(parsed, list):
        rows = [row for row in parsed if isinstance(row, dict)]
    elif isinstance(parsed, dict):
        rows = [parsed]
    else:
        rows = []

    with _OPENCLI_CACHE_LOCK:
        _OPENCLI_JSON_CACHE[cache_key] = (now_value, [dict(row) for row in rows])
        if len(_OPENCLI_JSON_CACHE) > 256:
            oldest_key = min(_OPENCLI_JSON_CACHE.items(), key=lambda item: item[1][0])[0]
            _OPENCLI_JSON_CACHE.pop(oldest_key, None)
    return [dict(row) for row in rows]


def get_opencli_status() -> dict[str, object]:
    opencli_bin = find_opencli_bin()
    if not opencli_bin:
        return {
            "ok": True,
            "installed": False,
            "connected": False,
            "message": "未安装 opencli",
            "hint": "先执行 npm install -g @jackwener/opencli",
        }

    cached = _get_status_cache(fresh_only=True)
    if cached:
        return cached

    version_proc = run_opencli_command(["--version"], timeout=12, allow_self_heal=False)
    doctor_text = ""
    connected = False
    connection_source = "doctor"
    last_error = ""
    chrome_window_count = _chrome_window_count()
    auto_recover_on_demand = bool(
        OPENCLI_SELF_HEAL_ENABLED
        and isinstance(chrome_window_count, int)
        and chrome_window_count == 0
    )

    for attempt in range(3):
        try:
            doctor_proc = run_opencli_command(
                ["doctor"],
                timeout=25,
                browser_lock_timeout=2.5,
                allow_self_heal=False,
            )
        except OpencliBusyError as exc:
            last_error = str(exc)
            break
        doctor_text = f"{doctor_proc.stdout}\n{doctor_proc.stderr}".strip()
        connected = "[OK] Extension: connected" in doctor_text
        if connected:
            break
        if attempt < 2:
            time.sleep(0.8)

    recent_success = _recent_opencli_success()
    if not connected and recent_success:
        connected = True
        connection_source = "recent_success"
        doctor_note = "opencli 最近一次 X 抓取成功，沿用可用状态。"
        doctor_text = f"{doctor_text}\n{doctor_note}".strip()

    if not connected and last_error:
        stale = _get_status_cache(fresh_only=False)
        if stale:
            stale["status_stale"] = True
            stale["message"] = "状态沿用最近结果（opencli 正在忙）"
            stale["last_status_error"] = last_error
            return _set_status_cache(stale)

    status_message = "已连通" if connection_source == "doctor" and connected else (
        "最近抓取成功" if connected else "未连通浏览器扩展"
    )
    status_hint = None if connected else "请在 Chrome 安装并启用 opencli Browser Bridge 扩展，然后重试。"
    if not connected and auto_recover_on_demand:
        status_message = "待机中，抓取时自动恢复"
        status_hint = "当前未打开 Chrome 窗口；生成日报或执行抓取时会自动拉起最小窗口，无需手动处理。"

    payload: dict[str, Any] = {
        "ok": True,
        "installed": True,
        "connected": connected,
        "binary": opencli_bin,
        "version": (version_proc.stdout or version_proc.stderr).strip(),
        "doctor": doctor_text,
        "message": status_message,
        "hint": status_hint,
        "connection_source": connection_source,
        "last_success_at": recent_success.get("at") if recent_success else None,
        "last_success_command": " ".join(recent_success.get("args") or []) if recent_success else "",
        "auto_recover_on_demand": auto_recover_on_demand,
        "chrome_window_count": chrome_window_count,
    }
    if last_error:
        payload["last_status_error"] = last_error
    return _set_status_cache(payload)
