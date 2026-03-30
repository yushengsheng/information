from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
import time
from contextlib import contextmanager
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app_config import (
    DATA_DIR,
    INTEL_CONFIG_FILE,
    INTEL_BUILD_TASK_FILE,
    INTEL_DELIVERY_STATE_FILE,
    INTEL_EVENT_POOL_FILE,
    INTEL_HISTORY_FILE,
    INTEL_LATEST_FILE,
    INTEL_LATEST_SENT_FILE,
    INTEL_OBSERVABILITY_HISTORY_FILE,
    INTEL_SECRETS_FILE,
    INTEL_SNAPSHOT_POOL_FILE,
    INTEL_SENT_FILE,
)
from services.intel.topics import build_default_fixed_topics

HISTORY_RETENTION_DAYS = 3
SNAPSHOT_POOL_RETENTION_SECONDS = HISTORY_RETENTION_DAYS * 24 * 60 * 60
EVENT_POOL_RETENTION_SECONDS = HISTORY_RETENTION_DAYS * 24 * 60 * 60
SENT_RETENTION_SECONDS = HISTORY_RETENTION_DAYS * 24 * 60 * 60
OBSERVABILITY_RETENTION_SECONDS = HISTORY_RETENTION_DAYS * 24 * 60 * 60
_DIGEST_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _store_lock_path():
    return DATA_DIR / ".intel_store.lock"


@contextmanager
def _store_file_lock(*, exclusive: bool):
    ensure_data_dir()
    with open(_store_lock_path(), "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_json_unlocked(path) -> object:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json_unlocked(path, payload: object) -> None:
    ensure_data_dir()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    fd, temp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass


def _read_json_locked(path) -> object:
    with _store_file_lock(exclusive=False):
        return _read_json_unlocked(path)


def _clamp_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return min(max(parsed, minimum), maximum)


def default_intel_config() -> dict[str, object]:
    return {
        "config_version": 3,
        "timezone": "Asia/Shanghai",
        "daily_push_time": "08:00",
        "daily_enabled": True,
        "limits": {
            "crypto": 10,
            "world": 3,
            "hot": 2,
            "custom_user": 3,
        },
        "telegram": {
            "enabled": True,
            "chat_id": "",
            "chat_title": "",
            "chat_type": "",
            "bot_username": "",
        },
        "summary": {
            "mode": "ai_first",
            "model": "gpt-5.4",
        },
        "fixed": build_default_fixed_topics(),
        "custom": {
            "x_users": [],
        },
    }


def merge_dict(base: dict[str, object], patch: dict[str, object]) -> dict[str, object]:
    result = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_dict(result[key], value)  # type: ignore[arg-type]
        else:
            result[key] = value
    return result


def normalize_summary_config(raw: object) -> dict[str, str]:
    summary = raw if isinstance(raw, dict) else {}
    mode = str(summary.get("mode") or "ai_first").strip().lower()
    if mode in {"fallback_only", "rule_only", "fallback"}:
        mode = "fallback_only"
    else:
        mode = "ai_first"
    model = str(summary.get("model") or "gpt-5.4").strip() or "gpt-5.4"
    return {"mode": mode, "model": model}


def load_intel_config() -> dict[str, object]:
    ensure_data_dir()
    base = default_intel_config()
    with _store_file_lock(exclusive=True):
        raw = _read_json_unlocked(INTEL_CONFIG_FILE)
        if raw is None:
            _write_json_unlocked(INTEL_CONFIG_FILE, base)
            return base
        if not isinstance(raw, dict):
            raw = {}
        return merge_dict(base, raw)


def save_intel_config(patch: dict[str, object]) -> dict[str, object]:
    base = default_intel_config()
    with _store_file_lock(exclusive=True):
        raw = _read_json_unlocked(INTEL_CONFIG_FILE)
        cfg = merge_dict(base, raw if isinstance(raw, dict) else {})
        merged = merge_dict(cfg, patch)

        timezone_name = str(merged.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            timezone_name = "Asia/Shanghai"
        merged["timezone"] = timezone_name

        daily_push_time = str(merged.get("daily_push_time") or "08:00").strip()
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", daily_push_time):
            daily_push_time = "08:00"
        merged["daily_push_time"] = daily_push_time
        merged["daily_enabled"] = bool(merged.get("daily_enabled", True))

        limits = merged.get("limits")
        if not isinstance(limits, dict):
            limits = {}
        limits["crypto"] = _clamp_int(limits.get("crypto"), 10, 1, 30)
        limits["world"] = _clamp_int(limits.get("world"), 3, 1, 20)
        limits["hot"] = _clamp_int(limits.get("hot"), 2, 0, 8)
        limits["custom_user"] = _clamp_int(limits.get("custom_user"), 3, 1, 10)
        merged["limits"] = limits

        custom = merged.get("custom")
        if not isinstance(custom, dict):
            custom = {}
        users = custom.get("x_users", [])
        if not isinstance(users, list):
            users = []
        sanitized_users: list[str] = []
        seen_users: set[str] = set()
        for value in users:
            username = str(value or "").strip().lstrip("@")
            if not username:
                continue
            if not re.fullmatch(r"[A-Za-z0-9_]{1,32}", username):
                continue
            username = username.lower()
            if username in seen_users:
                continue
            seen_users.add(username)
            sanitized_users.append(username)
        custom["x_users"] = sanitized_users
        merged["custom"] = custom

        telegram = merged.get("telegram")
        if not isinstance(telegram, dict):
            telegram = {}
        chat_id = str(telegram.get("chat_id") or "").strip()
        chat_title = str(telegram.get("chat_title") or "").strip()
        chat_type = str(telegram.get("chat_type") or "").strip()
        bot_username = str(telegram.get("bot_username") or "").strip().lstrip("@")
        telegram["enabled"] = bool(telegram.get("enabled", True))
        telegram["chat_id"] = chat_id
        telegram["chat_title"] = chat_title
        telegram["chat_type"] = chat_type
        telegram["bot_username"] = bot_username
        merged["telegram"] = telegram

        merged["summary"] = normalize_summary_config(merged.get("summary"))

        _write_json_unlocked(INTEL_CONFIG_FILE, merged)
    return merged


def load_sent_registry() -> dict[str, object]:
    ensure_data_dir()
    with _store_file_lock(exclusive=True):
        payload = _read_json_unlocked(INTEL_SENT_FILE)
        if not isinstance(payload, dict):
            return {"keys": {}, "updated_at": 0, "retention_seconds": SENT_RETENTION_SECONDS}
        normalized = _normalize_sent_registry_payload(payload)
        if payload != normalized:
            _write_json_unlocked(INTEL_SENT_FILE, normalized)
        return normalized


def save_sent_registry(payload: dict[str, object]) -> None:
    normalized = _normalize_sent_registry_payload(payload)
    with _store_file_lock(exclusive=True):
        _write_json_unlocked(INTEL_SENT_FILE, normalized)


def _normalize_sent_registry_payload(payload: object) -> dict[str, object]:
    data = payload if isinstance(payload, dict) else {}
    raw_keys = data.get("keys") if isinstance(data.get("keys"), dict) else {}
    cutoff_ts = int(time.time()) - SENT_RETENTION_SECONDS
    normalized_keys: dict[str, int] = {}
    for raw_key, raw_value in raw_keys.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        try:
            ts = int(raw_value)
        except Exception:
            continue
        if ts < cutoff_ts:
            continue
        normalized_keys[key] = ts
    updated_at_raw = data.get("updated_at")
    try:
        updated_at = int(updated_at_raw or 0)
    except Exception:
        updated_at = 0
    if updated_at and updated_at < cutoff_ts and normalized_keys:
        updated_at = max(normalized_keys.values(), default=updated_at)
    return {
        "keys": normalized_keys,
        "updated_at": max(updated_at, 0),
        "retention_seconds": SENT_RETENTION_SECONDS,
    }


def _extract_digest_date(payload: dict[str, object]) -> str:
    digest_date = str(payload.get("digest_date") or "").strip()
    if _DIGEST_DATE_PATTERN.fullmatch(digest_date):
        return digest_date

    generated_at = str(payload.get("generated_at") or "").strip()
    match = _DIGEST_DATE_PATTERN.search(generated_at)
    if match:
        return match.group(0)
    return ""


def _normalize_digest_payload(payload: object) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    normalized = dict(payload)
    digest_date = _extract_digest_date(normalized)
    if not digest_date:
        return None
    normalized["digest_date"] = digest_date
    normalized["exists"] = True
    return normalized


def _read_digest_file(path) -> dict[str, object] | None:
    data = _read_json_locked(path)
    if isinstance(data, dict):
        return data
    return None


def _read_latest_digest_file() -> dict[str, object] | None:
    return _read_digest_file(INTEL_LATEST_FILE)


def _normalize_history_items(items: list[object]) -> list[dict[str, object]]:
    normalized_items: list[dict[str, object]] = []
    for item in items:
        payload = None
        if isinstance(item, dict) and isinstance(item.get("payload"), dict):
            payload = _normalize_digest_payload(item.get("payload"))
        else:
            payload = _normalize_digest_payload(item)
        if not payload:
            continue
        normalized_items.append(
            {
                "digest_date": str(payload["digest_date"]),
                "payload": payload,
            }
        )

    normalized_items.sort(
        key=lambda entry: (
            str(entry.get("digest_date") or ""),
            str((entry.get("payload") if isinstance(entry.get("payload"), dict) else {}).get("generated_at") or ""),
        ),
        reverse=True,
    )

    deduped: list[dict[str, object]] = []
    seen_dates: set[str] = set()
    for entry in normalized_items:
        digest_date = str(entry.get("digest_date") or "")
        if not digest_date or digest_date in seen_dates:
            continue
        seen_dates.add(digest_date)
        deduped.append(entry)
        if len(deduped) >= HISTORY_RETENTION_DAYS:
            break
    return deduped


def _write_digest_history_items(items: list[dict[str, object]]) -> None:
    payload = {
        "version": 1,
        "retention_days": HISTORY_RETENTION_DAYS,
        "items": items,
    }
    _write_json_unlocked(INTEL_HISTORY_FILE, payload)


def _load_digest_history_items() -> list[dict[str, object]]:
    with _store_file_lock(exclusive=True):
        raw_items: list[object] = []
        payload = _read_json_unlocked(INTEL_HISTORY_FILE)
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            raw_items = list(payload.get("items") or [])

        if not raw_items:
            latest_sent = _read_json_unlocked(INTEL_LATEST_SENT_FILE)
            if isinstance(latest_sent, dict):
                raw_items = [latest_sent]
            else:
                legacy_latest = _read_json_unlocked(INTEL_LATEST_FILE)
                if isinstance(legacy_latest, dict) and bool(legacy_latest.get("final")):
                    raw_items = [legacy_latest]

        normalized_items = _normalize_history_items(raw_items)
        if normalized_items and (not INTEL_HISTORY_FILE.exists() or raw_items != normalized_items):
            _write_digest_history_items(normalized_items)
        return normalized_items


def list_digest_dates() -> list[str]:
    return [str(entry.get("digest_date") or "") for entry in _load_digest_history_items()]


def load_latest_sent_digest() -> dict[str, object] | None:
    latest_sent = _normalize_digest_payload(_read_digest_file(INTEL_LATEST_SENT_FILE))
    if latest_sent:
        result = dict(latest_sent)
        result["final"] = bool(result.get("final", True))
        return result

    history_items = _load_digest_history_items()
    if history_items:
        payload = history_items[0].get("payload")
        if isinstance(payload, dict):
            result = dict(payload)
            result["exists"] = True
            result["final"] = bool(result.get("final", True))
            return result

    legacy_latest = _normalize_digest_payload(_read_latest_digest_file())
    if legacy_latest and bool(legacy_latest.get("final")):
        result = dict(legacy_latest)
        result["final"] = True
        return result
    return None


def _attach_sent_payload(result: dict[str, object], sent_payload: dict[str, object] | None) -> dict[str, object]:
    enriched = dict(result)
    if not isinstance(sent_payload, dict):
        enriched["sent_exists"] = False
        return enriched
    enriched["sent_exists"] = True
    enriched["sent_digest_date"] = str(sent_payload.get("digest_date") or "")
    enriched["sent_sections"] = dict(sent_payload.get("sections") or {}) if isinstance(sent_payload.get("sections"), dict) else {}
    enriched["sent_final"] = bool(sent_payload.get("final", True))
    enriched["sent_generated_at"] = str(sent_payload.get("generated_at") or "")
    return enriched


def load_latest_digest(selected_date: str | None = None) -> dict[str, object]:
    history_items = _load_digest_history_items()
    available_dates = [str(entry.get("digest_date") or "") for entry in history_items]
    latest_sent = load_latest_sent_digest()

    if selected_date:
        for entry in history_items:
            if str(entry.get("digest_date") or "") != selected_date:
                continue
            payload = entry.get("payload")
            if isinstance(payload, dict):
                result = dict(payload)
                result["exists"] = True
                result["selected_date"] = selected_date
                result["available_dates"] = available_dates
                result["display_source"] = "history"
                return _attach_sent_payload(result, result)
        return _attach_sent_payload({
            "ok": True,
            "exists": False,
            "selected_date": selected_date,
            "available_dates": available_dates,
            "display_source": "history",
        }, latest_sent)

    latest_payload = _normalize_digest_payload(_read_latest_digest_file())
    if latest_payload:
        result = dict(latest_payload)
        result["exists"] = True
        result["selected_date"] = ""
        result["available_dates"] = available_dates
        result["display_source"] = "snapshot"
        return _attach_sent_payload(result, latest_sent)

    if latest_sent:
        result = dict(latest_sent)
        result["exists"] = True
        result["selected_date"] = ""
        result["available_dates"] = available_dates
        result["display_source"] = "latest_sent"
        return _attach_sent_payload(result, latest_sent)

    if history_items:
        payload = history_items[0].get("payload")
        if isinstance(payload, dict):
            result = dict(payload)
            result["exists"] = True
            result["selected_date"] = str(history_items[0].get("digest_date") or "")
            result["available_dates"] = available_dates
            result["display_source"] = "history"
            return _attach_sent_payload(result, result)

    return _attach_sent_payload({
        "ok": True,
        "exists": False,
        "selected_date": "",
        "available_dates": available_dates,
        "display_source": "none",
    }, latest_sent)


def save_latest_digest(payload: dict[str, object], *, persist_history: bool = True) -> None:
    normalized_payload = _normalize_digest_payload(payload) or dict(payload)
    with _store_file_lock(exclusive=True):
        _write_json_unlocked(INTEL_LATEST_FILE, normalized_payload)

        if not persist_history:
            return

        digest_date = _extract_digest_date(normalized_payload)
        if not digest_date:
            return

        normalized_payload["digest_date"] = digest_date
        history_payload = _read_json_unlocked(INTEL_HISTORY_FILE)
        raw_items = list(history_payload.get("items") or []) if isinstance(history_payload, dict) and isinstance(history_payload.get("items"), list) else []
        current_items = _normalize_history_items(raw_items)
        current_items = [entry for entry in current_items if str(entry.get("digest_date") or "") != digest_date]
        current_items.append({"digest_date": digest_date, "payload": normalized_payload})
        _write_digest_history_items(_normalize_history_items(current_items))


def save_latest_sent_digest(payload: dict[str, object]) -> None:
    normalized_payload = _normalize_digest_payload(payload) or dict(payload)
    normalized_payload["final"] = True
    with _store_file_lock(exclusive=True):
        _write_json_unlocked(INTEL_LATEST_SENT_FILE, normalized_payload)

        digest_date = _extract_digest_date(normalized_payload)
        if not digest_date:
            return
        normalized_payload["digest_date"] = digest_date
        history_payload = _read_json_unlocked(INTEL_HISTORY_FILE)
        raw_items = list(history_payload.get("items") or []) if isinstance(history_payload, dict) and isinstance(history_payload.get("items"), list) else []
        current_items = _normalize_history_items(raw_items)
        current_items = [entry for entry in current_items if str(entry.get("digest_date") or "") != digest_date]
        current_items.append({"digest_date": digest_date, "payload": normalized_payload})
        _write_digest_history_items(_normalize_history_items(current_items))


def _normalize_observability_history_items(items: list[object]) -> list[dict[str, object]]:
    cutoff_ts = int(time.time()) - OBSERVABILITY_RETENTION_SECONDS
    normalized: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            captured_at = int(item.get("captured_at") or 0)
        except Exception:
            captured_at = 0
        if captured_at <= 0 or captured_at < cutoff_ts:
            continue
        level = str(item.get("level") or "").strip().lower()
        if level not in {"ok", "info", "warn", "critical"}:
            level = "ok"
        alert_codes_raw = item.get("alert_codes") if isinstance(item.get("alert_codes"), list) else []
        alert_codes = [str(code).strip().lower() for code in alert_codes_raw if str(code).strip()]
        repeats_raw = item.get("repeat_codes") if isinstance(item.get("repeat_codes"), dict) else {}
        repeat_codes: dict[str, int] = {}
        for code, count in repeats_raw.items():
            key = str(code or "").strip().lower()
            if not key:
                continue
            try:
                repeat_codes[key] = max(int(count or 0), 0)
            except Exception:
                repeat_codes[key] = 0
        normalized.append(
            {
                "captured_at": captured_at,
                "source": str(item.get("source") or "").strip().lower(),
                "level": level,
                "issue_count": max(int(item.get("issue_count") or len(alert_codes)), 0),
                "alert_codes": alert_codes,
                "primary_code": str(item.get("primary_code") or "").strip().lower(),
                "payload_source": str(item.get("payload_source") or "").strip().lower(),
                "rss_share": round(max(float(item.get("rss_share") or 0.0), 0.0), 4),
                "fallback_ratio": round(max(float(item.get("fallback_ratio") or 0.0), 0.0), 4),
                "x_total_configured": max(int(item.get("x_total_configured") or 0), 0),
                "opencli_state": str(item.get("opencli_state") or "").strip().lower(),
                "repeat_codes": repeat_codes,
            }
        )
    normalized.sort(key=lambda entry: int(entry.get("captured_at") or 0))
    return normalized


def load_observability_history(*, limit: int | None = None) -> list[dict[str, object]]:
    with _store_file_lock(exclusive=True):
        raw_items: list[object] = []
        payload = _read_json_unlocked(INTEL_OBSERVABILITY_HISTORY_FILE)
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            raw_items = list(payload.get("items") or [])
        normalized_items = _normalize_observability_history_items(raw_items)
        if raw_items != normalized_items:
            _write_json_unlocked(
                INTEL_OBSERVABILITY_HISTORY_FILE,
                {
                    "version": 1,
                    "retention_seconds": OBSERVABILITY_RETENTION_SECONDS,
                    "items": normalized_items,
                },
            )
        if isinstance(limit, int) and limit > 0:
            return normalized_items[-limit:]
        return normalized_items


def save_observability_history_entry(entry: dict[str, object]) -> None:
    with _store_file_lock(exclusive=True):
        payload = _read_json_unlocked(INTEL_OBSERVABILITY_HISTORY_FILE)
        current_items = list(payload.get("items") or []) if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
        current_items.append(dict(entry) if isinstance(entry, dict) else {})
        normalized_items = _normalize_observability_history_items(current_items)
        _write_json_unlocked(
            INTEL_OBSERVABILITY_HISTORY_FILE,
            {
                "version": 1,
                "retention_seconds": OBSERVABILITY_RETENTION_SECONDS,
                "items": normalized_items,
            },
        )


def _normalize_snapshot_pool_items(items: list[object]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    cutoff_ts = int(time.time()) - SNAPSHOT_POOL_RETENTION_SECONDS
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            captured_at = int(item.get("captured_at") or 0)
        except Exception:
            captured_at = 0
        if captured_at <= 0 or captured_at < cutoff_ts:
            continue
        topics = item.get("topics")
        if not isinstance(topics, dict):
            continue
        normalized_topics: dict[str, list[dict[str, object]]] = {}
        for topic in ("crypto", "world", "hot", "custom"):
            topic_items = topics.get(topic)
            if not isinstance(topic_items, list):
                normalized_topics[topic] = []
                continue
            normalized_topics[topic] = [dict(row) for row in topic_items if isinstance(row, dict)]
        normalized.append(
            {
                "captured_at": captured_at,
                "generated_at": str(item.get("generated_at") or ""),
                "digest_date": str(item.get("digest_date") or ""),
                "topics": normalized_topics,
            }
        )
    normalized.sort(key=lambda entry: int(entry.get("captured_at") or 0))
    return normalized


def _load_snapshot_pool_items() -> list[dict[str, object]]:
    with _store_file_lock(exclusive=True):
        raw_items: list[object] = []
        payload = _read_json_unlocked(INTEL_SNAPSHOT_POOL_FILE)
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            raw_items = list(payload.get("items") or [])
        normalized_items = _normalize_snapshot_pool_items(raw_items)
        if raw_items != normalized_items:
            _write_json_unlocked(
                INTEL_SNAPSHOT_POOL_FILE,
                {
                    "version": 1,
                    "retention_seconds": SNAPSHOT_POOL_RETENTION_SECONDS,
                    "items": normalized_items,
                },
            )
        return normalized_items


def save_snapshot_pool_entry(topics: dict[str, list[dict[str, object]]], *, captured_at: int | None = None, generated_at: str = "", digest_date: str = "") -> None:
    with _store_file_lock(exclusive=True):
        payload = _read_json_unlocked(INTEL_SNAPSHOT_POOL_FILE)
        current_items = list(payload.get("items") or []) if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
        current_items.append(
            {
                "captured_at": int(captured_at or time.time()),
                "generated_at": str(generated_at or ""),
                "digest_date": str(digest_date or ""),
                "topics": {
                    topic: [dict(row) for row in rows if isinstance(row, dict)]
                    for topic, rows in (topics.items() if isinstance(topics, dict) else [])
                    if topic in {"crypto", "world", "hot", "custom"} and isinstance(rows, list)
                },
            }
        )
        normalized_items = _normalize_snapshot_pool_items(current_items)
        _write_json_unlocked(
            INTEL_SNAPSHOT_POOL_FILE,
            {
                "version": 1,
                "retention_seconds": SNAPSHOT_POOL_RETENTION_SECONDS,
                "items": normalized_items,
            },
        )


def load_snapshot_pool_since(since_ts: int) -> list[dict[str, object]]:
    try:
        cutoff = int(since_ts)
    except Exception:
        cutoff = 0
    return [entry for entry in _load_snapshot_pool_items() if int(entry.get("captured_at") or 0) >= cutoff]


def _normalize_event_pool_items(items: list[object]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    cutoff_ts = int(time.time()) - EVENT_POOL_RETENTION_SECONDS
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            captured_at = int(item.get("captured_at") or 0)
        except Exception:
            captured_at = 0
        if captured_at <= 0 or captured_at < cutoff_ts:
            continue
        event_key = str(item.get("event_key") or "").strip()
        topic = str(item.get("topic") or "").strip()
        if not event_key or topic not in {"crypto", "world", "hot", "custom"}:
            continue
        representatives = item.get("representatives")
        if not isinstance(representatives, dict):
            continue
        normalized_reps: dict[str, dict[str, object]] = {}
        for source, record in representatives.items():
            source_name = str(source or "").strip().lower()
            if source_name not in {"x", "rss", "reddit"} or not isinstance(record, dict):
                continue
            normalized_reps[source_name] = dict(record)
        if not normalized_reps:
            continue
        source_counts = item.get("source_counts")
        normalized_counts: dict[str, int] = {}
        if isinstance(source_counts, dict):
            for source, value in source_counts.items():
                source_name = str(source or "").strip().lower()
                if source_name not in {"x", "rss", "reddit"}:
                    continue
                try:
                    normalized_counts[source_name] = max(int(value or 0), 0)
                except Exception:
                    normalized_counts[source_name] = 0
        normalized.append(
            {
                "captured_at": captured_at,
                "generated_at": str(item.get("generated_at") or ""),
                "digest_date": str(item.get("digest_date") or ""),
                "topic": topic,
                "event_key": event_key,
                "signature_tokens": [str(token).strip() for token in (item.get("signature_tokens") or []) if str(token).strip()],
                "snapshot_hits": max(int(item.get("snapshot_hits") or 1), 1),
                "evidence_count": max(int(item.get("evidence_count") or 0), 0),
                "source_counts": normalized_counts,
                "first_seen_at": max(int(item.get("first_seen_at") or captured_at), 0),
                "last_seen_at": max(int(item.get("last_seen_at") or captured_at), 0),
                "confirmation_count": max(int(item.get("confirmation_count") or 0), 0),
                "authority_score": max(float(item.get("authority_score") or 0.0), 0.0),
                "authority_keys": [
                    str(key).strip().lower()
                    for key in (item.get("authority_keys") or [])
                    if str(key).strip()
                ],
                "representatives": normalized_reps,
            }
        )
    normalized.sort(key=lambda entry: (int(entry.get("captured_at") or 0), str(entry.get("topic") or ""), str(entry.get("event_key") or "")))
    return normalized


def _load_event_pool_items() -> list[dict[str, object]]:
    with _store_file_lock(exclusive=True):
        raw_items: list[object] = []
        payload = _read_json_unlocked(INTEL_EVENT_POOL_FILE)
        if isinstance(payload, dict) and isinstance(payload.get("items"), list):
            raw_items = list(payload.get("items") or [])
        normalized_items = _normalize_event_pool_items(raw_items)
        if raw_items != normalized_items:
            _write_json_unlocked(
                INTEL_EVENT_POOL_FILE,
                {
                    "version": 1,
                    "retention_seconds": EVENT_POOL_RETENTION_SECONDS,
                    "items": normalized_items,
                },
            )
        return normalized_items


def save_event_pool_entries(entries: list[dict[str, object]]) -> None:
    with _store_file_lock(exclusive=True):
        payload = _read_json_unlocked(INTEL_EVENT_POOL_FILE)
        current_items = list(payload.get("items") or []) if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
        current_items.extend(dict(entry) for entry in entries if isinstance(entry, dict))
        normalized_items = _normalize_event_pool_items(current_items)
        _write_json_unlocked(
            INTEL_EVENT_POOL_FILE,
            {
                "version": 1,
                "retention_seconds": EVENT_POOL_RETENTION_SECONDS,
                "items": normalized_items,
            },
        )


def load_event_pool_since(since_ts: int) -> list[dict[str, object]]:
    try:
        cutoff = int(since_ts)
    except Exception:
        cutoff = 0
    return [entry for entry in _load_event_pool_items() if int(entry.get("captured_at") or 0) >= cutoff]


def load_intel_secrets() -> dict[str, object]:
    data = _read_json_locked(INTEL_SECRETS_FILE)
    if isinstance(data, dict):
        return data
    return {"telegram_bot_token": "", "summary_api_key": "", "summary_base_url": "", "summary_provider": ""}


def save_intel_secrets(patch: dict[str, object]) -> dict[str, object]:
    with _store_file_lock(exclusive=True):
        current = _read_json_unlocked(INTEL_SECRETS_FILE)
        merged = merge_dict(current if isinstance(current, dict) else {}, patch)
        token = str(merged.get("telegram_bot_token") or "").strip()
        merged["telegram_bot_token"] = token
        merged["summary_api_key"] = str(merged.get("summary_api_key") or "").strip()
        merged["summary_base_url"] = str(merged.get("summary_base_url") or "").strip()
        merged["summary_provider"] = str(merged.get("summary_provider") or "").strip()
        _write_json_unlocked(INTEL_SECRETS_FILE, merged)
    return merged


def mask_secret(value: str) -> str:
    secret = str(value or "").strip()
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}...{secret[-4:]}"


def load_delivery_state() -> dict[str, object]:
    data = _read_json_locked(INTEL_DELIVERY_STATE_FILE)
    if isinstance(data, dict):
        return data
    return {"daily_digest": {"last_sent_date": "", "last_sent_at": "", "last_error": "", "last_attempt_at": ""}}


def save_delivery_state(payload: dict[str, object]) -> None:
    with _store_file_lock(exclusive=True):
        _write_json_unlocked(INTEL_DELIVERY_STATE_FILE, payload)


def update_delivery_state(mutator) -> dict[str, object]:
    with _store_file_lock(exclusive=True):
        current = _read_json_unlocked(INTEL_DELIVERY_STATE_FILE)
        if not isinstance(current, dict):
            current = {"daily_digest": {"last_sent_date": "", "last_sent_at": "", "last_error": "", "last_attempt_at": ""}}
        updated = mutator(dict(current))
        if not isinstance(updated, dict):
            updated = current
        _write_json_unlocked(INTEL_DELIVERY_STATE_FILE, updated)
        return updated


def load_intel_build_task_state() -> dict[str, object]:
    data = _read_json_locked(INTEL_BUILD_TASK_FILE)
    if isinstance(data, dict):
        return data
    return {"task": None}


def save_intel_build_task_state(payload: dict[str, object]) -> None:
    with _store_file_lock(exclusive=True):
        _write_json_unlocked(INTEL_BUILD_TASK_FILE, payload)
