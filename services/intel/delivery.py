from __future__ import annotations

import hashlib
import html
import time
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app_config import APP_VERSION
from monitor import format_clock
from app_config import INTEL_DAILY_AGGREGATE_WINDOW_SECONDS
from services.intel.digest import (
    build_event_pool_candidate_pool,
    build_digest_payload,
    collect_digest_candidates,
    finalize_digest_payload,
    get_digest_message_html,
)
from services.intel.store import (
    load_delivery_state,
    load_intel_config,
    load_intel_secrets,
    mask_secret,
    save_delivery_state,
    save_intel_config,
    update_delivery_state,
)
from services.intel.telegram import get_telegram_me, resolve_latest_chat, send_telegram_message
from services.intel.text import normalize_text

DEFAULT_DAILY_PUSH_TIME = "08:00"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DAILY_X_RETRY_DELAYS_SECONDS = (20.0, 45.0)
DAILY_OVERDUE_WARNING_SECONDS = 5 * 60
DAILY_OVERDUE_FALLBACK_SECONDS = 15 * 60


def _safe_timezone_name(value: object) -> str:
    candidate = normalize_text(value) or DEFAULT_TIMEZONE
    try:
        ZoneInfo(candidate)
        return candidate
    except ZoneInfoNotFoundError:
        return DEFAULT_TIMEZONE


def _parse_daily_time(value: object) -> tuple[int, int]:
    raw = normalize_text(value) or DEFAULT_DAILY_PUSH_TIME
    try:
        hour_text, minute_text = raw.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass
    return 8, 0


def _load_daily_state() -> tuple[dict[str, object], dict[str, object]]:
    delivery_state = load_delivery_state()
    daily_state = delivery_state.get("daily_digest") if isinstance(delivery_state.get("daily_digest"), dict) else {}
    return delivery_state, dict(daily_state)


def _save_daily_state(patch: dict[str, object]) -> dict[str, object]:
    def _mutate(delivery_state: dict[str, object]) -> dict[str, object]:
        daily_state = delivery_state.get("daily_digest") if isinstance(delivery_state.get("daily_digest"), dict) else {}
        merged = {**daily_state, **patch}
        delivery_state["daily_digest"] = merged
        return delivery_state

    updated_state = update_delivery_state(_mutate)
    updated_daily = updated_state.get("daily_digest") if isinstance(updated_state.get("daily_digest"), dict) else {}
    return dict(updated_daily)


def _parse_state_timestamp(value: object) -> int | None:
    raw = normalize_text(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return None
    return int(parsed.timestamp())


def _normalize_pending_delivery(value: object) -> dict[str, object]:
    pending = value if isinstance(value, dict) else {}
    state = normalize_text(pending.get("state")).lower()
    if state not in {"sending", "sent_not_committed"}:
        return {}
    message_ids = pending.get("message_ids") if isinstance(pending.get("message_ids"), list) else []
    normalized_ids: list[int] = []
    for message_id in message_ids:
        try:
            normalized_ids.append(int(message_id))
        except Exception:
            continue
    return {
        "state": state,
        "digest_date": normalize_text(pending.get("digest_date")),
        "prepared_at": normalize_text(pending.get("prepared_at")),
        "sent_at": normalize_text(pending.get("sent_at")),
        "attempt_id": normalize_text(pending.get("attempt_id")),
        "message_fingerprint": normalize_text(pending.get("message_fingerprint")),
        "message_ids": normalized_ids,
        "chunk_count": max(len(normalized_ids), int(pending.get("chunk_count") or 0)),
    }


def _pending_delivery_message(pending: dict[str, object]) -> str:
    digest_date = normalize_text(pending.get("digest_date"))
    date_label = digest_date or "今日"
    if normalize_text(pending.get("state")).lower() == "sent_not_committed":
        return f"{date_label} 正式日报已发出，但本地确认未完成；为避免重复发送，系统已暂停自动重发。"
    return f"{date_label} 正式日报状态待确认；为避免重复发送，系统已暂停自动重发。"


def inspect_daily_delivery_window(
    *,
    cfg: dict[str, object] | None = None,
    telegram: dict[str, object] | None = None,
    token: str | None = None,
    daily_state: dict[str, object] | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    if cfg is None or telegram is None or token is None:
        cfg, telegram, loaded_token = _load_runtime_context()
        token = loaded_token if token is None else token
    cfg = cfg if isinstance(cfg, dict) else {}
    telegram = telegram if isinstance(telegram, dict) else {}
    daily_state = daily_state if isinstance(daily_state, dict) else {}

    timezone_name = _safe_timezone_name(cfg.get("timezone"))
    zone = ZoneInfo(timezone_name)
    if now is None:
        current = datetime.now(zone)
    elif now.tzinfo is None:
        current = now.replace(tzinfo=zone)
    else:
        current = now.astimezone(zone)

    due_hour, due_minute = _parse_daily_time(cfg.get("daily_push_time"))
    due_at = current.replace(hour=due_hour, minute=due_minute, second=0, microsecond=0)
    today = current.strftime("%Y-%m-%d")
    pending_delivery = _normalize_pending_delivery(daily_state.get("pending_delivery"))
    pending_today = bool(pending_delivery and normalize_text(pending_delivery.get("digest_date")) in {"", today})
    last_sent_date = normalize_text(daily_state.get("last_sent_date"))
    fallback_triggered_date = normalize_text(daily_state.get("fallback_triggered_date"))
    last_error = normalize_text(daily_state.get("last_error"))
    daily_enabled = bool(cfg.get("daily_enabled", True))
    telegram_enabled = bool(telegram.get("enabled", True))
    chat_ready = bool(normalize_text(token) and normalize_text(telegram.get("chat_id")))
    is_due = current >= due_at
    elapsed_since_due_seconds = max(int((current - due_at).total_seconds()), 0) if is_due else 0
    is_overdue = bool(
        daily_enabled
        and telegram_enabled
        and chat_ready
        and last_sent_date != today
        and not pending_today
        and elapsed_since_due_seconds >= DAILY_OVERDUE_WARNING_SECONDS
    )
    overdue_seconds = elapsed_since_due_seconds if is_overdue else 0
    overdue_minutes = overdue_seconds // 60 if overdue_seconds > 0 else 0
    fallback_attempted_today = fallback_triggered_date == today
    fallback_due = bool(is_overdue and elapsed_since_due_seconds >= DAILY_OVERDUE_FALLBACK_SECONDS and not fallback_attempted_today)

    status_text = ""
    if pending_today:
        status_text = _pending_delivery_message(pending_delivery)
    elif not daily_enabled or not telegram_enabled:
        status_text = "每日推送已关闭"
    elif not chat_ready:
        status_text = "Telegram 目标未就绪，今日无法自动发送日报"
    elif last_sent_date == today:
        status_text = f"今日已发送（{today}）"
    elif not is_due:
        status_text = f"等待今日 {normalize_text(cfg.get('daily_push_time')) or DEFAULT_DAILY_PUSH_TIME} 定时发送"
    elif is_overdue:
        status_text = f"今日日报已超过计划发送时间 {overdue_minutes} 分钟，仍未确认发送。"
        if fallback_due:
            status_text = f"{status_text} 已达到自动补偿阈值。"
        elif fallback_attempted_today:
            status_text = f"{status_text} 今日已执行过一次自动补偿。"
    else:
        status_text = "今日已到发送窗口，等待发送确认。"

    if last_error and is_overdue:
        status_text = f"{status_text} 最近错误：{last_error}"

    return {
        "today": today,
        "timezone": timezone_name,
        "due_at": due_at.isoformat(timespec="seconds"),
        "due_time": f"{due_hour:02d}:{due_minute:02d}",
        "pending_today": pending_today,
        "is_due": is_due,
        "is_overdue": is_overdue,
        "overdue_seconds": overdue_seconds,
        "overdue_minutes": overdue_minutes,
        "fallback_due": fallback_due,
        "fallback_attempted_today": fallback_attempted_today,
        "fallback_triggered_date": fallback_triggered_date,
        "fallback_last_attempt_at": normalize_text(daily_state.get("fallback_last_attempt_at")),
        "fallback_last_result": normalize_text(daily_state.get("fallback_last_result")),
        "status_text": status_text,
    }


def record_daily_fallback_attempt(*, digest_date: str, attempted_at: str, result_message: str) -> dict[str, object]:
    digest_day = normalize_text(digest_date)
    attempt_text = normalize_text(attempted_at)
    result_text = normalize_text(result_message)
    saved_state = _save_daily_state(
        {
            "fallback_triggered_date": digest_day,
            "fallback_last_attempt_at": attempt_text,
            "fallback_last_result": result_text,
        }
    )
    return dict(saved_state)


def _message_fingerprint(text: object) -> str:
    value = normalize_text(text)
    if not value:
        return ""
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def _extract_message_ids(results: object) -> list[int]:
    rows = results if isinstance(results, list) else []
    message_ids: list[int] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            message_ids.append(int(row.get("message_id")))
        except Exception:
            continue
    return message_ids


def _load_runtime_context() -> tuple[dict[str, object], dict[str, object], str]:
    cfg = load_intel_config()
    telegram = cfg.get("telegram") if isinstance(cfg.get("telegram"), dict) else {}
    secrets = load_intel_secrets()
    token = normalize_text(secrets.get("telegram_bot_token"))
    return cfg, telegram, token


def _configured_x_topics(cfg: dict[str, object]) -> set[str]:
    topics: set[str] = set()
    fixed = cfg.get("fixed") if isinstance(cfg.get("fixed"), dict) else {}
    for topic in ("crypto", "world", "hot"):
        topic_cfg = fixed.get(topic) if isinstance(fixed, dict) else {}
        x_queries = topic_cfg.get("x_queries") if isinstance(topic_cfg, dict) else []
        if isinstance(x_queries, list) and any(normalize_text(query) for query in x_queries):
            topics.add(topic)

    custom = cfg.get("custom") if isinstance(cfg.get("custom"), dict) else {}
    x_users = custom.get("x_users") if isinstance(custom, dict) else []
    if isinstance(x_users, list) and any(normalize_text(user) for user in x_users):
        topics.add("custom")
    return topics


def _payload_raw_x_count(payload: dict[str, object], topic: str) -> int:
    build_stats = payload.get("build_stats") if isinstance(payload.get("build_stats"), dict) else {}
    source_counts = build_stats.get("source_counts") if isinstance(build_stats.get("source_counts"), dict) else {}
    topic_counts = source_counts.get(topic) if isinstance(source_counts.get(topic), dict) else {}
    try:
        return max(int(topic_counts.get("x", 0) or 0), 0)
    except Exception:
        return 0


def _payload_needs_x_retry(cfg: dict[str, object], payload: dict[str, object]) -> bool:
    configured_topics = _configured_x_topics(cfg)
    if not configured_topics:
        return False
    return sum(_payload_raw_x_count(payload, topic) for topic in configured_topics) <= 0


def _event_window_since_ts(previous_sent_cutoff_ts: int | None, *, fallback_window_seconds: int = INTEL_DAILY_AGGREGATE_WINDOW_SECONDS) -> int:
    return int(previous_sent_cutoff_ts or (time.time() - fallback_window_seconds))


def _build_event_pool_payload(
    cfg: dict[str, object],
    *,
    previous_sent_cutoff_ts: int | None,
    respect_sent: bool,
) -> tuple[dict[str, object], dict[str, object]]:
    attempts: list[dict[str, object]] = []
    window_since_ts = _event_window_since_ts(previous_sent_cutoff_ts)
    live_candidates = collect_digest_candidates(cfg)
    aggregate_candidates, aggregate_meta = build_event_pool_candidate_pool(live_candidates, since_ts=window_since_ts)
    payload = build_digest_payload(
        final=False,
        respect_sent=respect_sent,
        sent_cutoff_ts=previous_sent_cutoff_ts,
        cfg=cfg,
        collected=aggregate_candidates,
        window_meta=aggregate_meta,
    )
    attempts.append(
        {
            "attempt": 1,
            "raw_x_total": sum(_payload_raw_x_count(payload, topic) for topic in _configured_x_topics(cfg)),
            "event_pool_entry_count": int(aggregate_meta.get("event_pool_entry_count", 0) or 0),
            "bootstrap_snapshot_count": int(aggregate_meta.get("bootstrap_snapshot_count", 0) or 0),
        }
    )
    if not _payload_needs_x_retry(cfg, payload):
        return payload, {
            "x_retry_triggered": False,
            "x_retry_attempts": attempts,
            "aggregate_window_since_ts": window_since_ts,
        }

    for index, delay_seconds in enumerate(DAILY_X_RETRY_DELAYS_SECONDS, start=2):
        time.sleep(max(delay_seconds, 0.0))
        live_candidates = collect_digest_candidates(cfg)
        aggregate_candidates, aggregate_meta = build_event_pool_candidate_pool(live_candidates, since_ts=window_since_ts)
        retried_payload = build_digest_payload(
            final=False,
            respect_sent=respect_sent,
            sent_cutoff_ts=previous_sent_cutoff_ts,
            cfg=cfg,
            collected=aggregate_candidates,
            window_meta=aggregate_meta,
        )
        attempts.append(
            {
                "attempt": index,
                "delay_seconds": delay_seconds,
                "raw_x_total": sum(_payload_raw_x_count(retried_payload, topic) for topic in _configured_x_topics(cfg)),
                "event_pool_entry_count": int(aggregate_meta.get("event_pool_entry_count", 0) or 0),
                "bootstrap_snapshot_count": int(aggregate_meta.get("bootstrap_snapshot_count", 0) or 0),
            }
        )
        payload = retried_payload
        if not _payload_needs_x_retry(cfg, payload):
            return payload, {
                "x_retry_triggered": True,
                "x_retry_attempts": attempts,
                "x_retry_recovered": True,
                "aggregate_window_since_ts": window_since_ts,
            }

    return payload, {
        "x_retry_triggered": True,
        "x_retry_attempts": attempts,
        "x_retry_recovered": False,
        "aggregate_window_since_ts": window_since_ts,
    }


def build_manual_digest_payload(final: bool = False, respect_sent: bool | None = None) -> dict[str, object]:
    cfg = load_intel_config()
    if respect_sent is None:
        respect_sent = final
    previous_sent_cutoff_ts = _parse_state_timestamp(load_delivery_state().get("daily_digest", {}).get("last_sent_at"))
    payload, build_meta = _build_event_pool_payload(
        cfg,
        previous_sent_cutoff_ts=previous_sent_cutoff_ts if respect_sent else None,
        respect_sent=bool(respect_sent),
    )
    payload["manual_build"] = True
    payload["build_stats"] = dict(payload.get("build_stats") or {})
    payload["build_stats"]["manual"] = {
        "final": bool(final),
        "respect_sent": bool(respect_sent),
        **build_meta,
    }
    if final:
        return finalize_digest_payload(payload)
    return payload


def _refresh_bot_username(cfg: dict[str, object], token: str) -> dict[str, object]:
    telegram = cfg.get("telegram") if isinstance(cfg.get("telegram"), dict) else {}
    if not token or normalize_text(telegram.get("bot_username")):
        return cfg

    bot_profile = get_telegram_me(token)
    bot_username = normalize_text(bot_profile.get("username"))
    if not bot_username:
        return cfg
    return save_intel_config({"telegram": {"bot_username": bot_username}})


def _auto_bind_chat(cfg: dict[str, object], token: str) -> dict[str, object]:
    telegram = cfg.get("telegram") if isinstance(cfg.get("telegram"), dict) else {}
    if not token or normalize_text(telegram.get("chat_id")):
        return cfg

    resolved = resolve_latest_chat(token)
    if not resolved or not normalize_text(resolved.get("chat_id")):
        return cfg
    return save_intel_config({"telegram": resolved})


def build_delivery_status(scheduler_status: dict[str, object] | None = None) -> dict[str, object]:
    cfg, telegram, token = _load_runtime_context()
    _, daily_state = _load_daily_state()
    bot_username = normalize_text(telegram.get("bot_username"))
    pending_delivery = _normalize_pending_delivery(daily_state.get("pending_delivery"))
    delivery_window = inspect_daily_delivery_window(
        cfg=cfg,
        telegram=telegram,
        token=token,
        daily_state=daily_state,
    )
    hint = f"如未绑定目标会话，请先给 @{bot_username or '你的机器人'} 发送 /start 或任意消息，然后点击“识别最近会话”。"
    if pending_delivery:
        hint = _pending_delivery_message(pending_delivery)
    elif bool(delivery_window.get("is_overdue")):
        hint = normalize_text(delivery_window.get("status_text")) or hint

    return {
        "ok": True,
        "telegram": {
            "enabled": bool(telegram.get("enabled", True)),
            "bot_token_configured": bool(token),
            "bot_token_masked": mask_secret(token) if token else "",
            "bot_username": bot_username,
            "chat_id": normalize_text(telegram.get("chat_id")),
            "chat_title": normalize_text(telegram.get("chat_title")),
            "chat_type": normalize_text(telegram.get("chat_type")),
            "chat_ready": bool(token and normalize_text(telegram.get("chat_id"))),
        },
        "daily": {
            "enabled": bool(cfg.get("daily_enabled", True)),
            "push_time": normalize_text(cfg.get("daily_push_time")) or DEFAULT_DAILY_PUSH_TIME,
            "timezone": _safe_timezone_name(cfg.get("timezone")),
        },
        "scheduler": dict(scheduler_status or {}),
        "delivery": {
            "today": normalize_text(delivery_window.get("today")),
            "last_sent_date": normalize_text(daily_state.get("last_sent_date")),
            "last_sent_at": normalize_text(daily_state.get("last_sent_at")),
            "last_error": normalize_text(daily_state.get("last_error")),
            "last_attempt_at": normalize_text(daily_state.get("last_attempt_at")),
            "pending_state": normalize_text(pending_delivery.get("state")),
            "pending_digest_date": normalize_text(pending_delivery.get("digest_date")),
            "pending_prepared_at": normalize_text(pending_delivery.get("prepared_at")),
            "pending_sent_at": normalize_text(pending_delivery.get("sent_at")),
            "pending_message_ids": list(pending_delivery.get("message_ids") or []),
            "pending_actions": ["confirm", "clear"] if pending_delivery else [],
            "due_at": normalize_text(delivery_window.get("due_at")),
            "is_due": bool(delivery_window.get("is_due")),
            "is_overdue": bool(delivery_window.get("is_overdue")),
            "overdue_minutes": max(int(delivery_window.get("overdue_minutes") or 0), 0),
            "fallback_due": bool(delivery_window.get("fallback_due")),
            "fallback_attempted_today": bool(delivery_window.get("fallback_attempted_today")),
            "fallback_last_attempt_at": normalize_text(delivery_window.get("fallback_last_attempt_at")),
            "fallback_last_result": normalize_text(delivery_window.get("fallback_last_result")),
            "status_text": normalize_text(delivery_window.get("status_text")),
        },
        "hint": hint,
        "app_version": APP_VERSION,
    }


def resolve_pending_daily_delivery(action: str) -> dict[str, object]:
    action_name = normalize_text(action).lower()
    if action_name not in {"confirm", "clear"}:
        raise ValueError("不支持的待确认处理动作")

    cfg = load_intel_config()
    timezone_name = _safe_timezone_name(cfg.get("timezone"))
    zone = ZoneInfo(timezone_name)
    now_text = datetime.now(zone).isoformat(timespec="seconds")
    _, daily_state = _load_daily_state()
    pending_delivery = _normalize_pending_delivery(daily_state.get("pending_delivery"))
    if not pending_delivery:
        return {
            **build_delivery_status(),
            "ok": False,
            "error": "当前没有待确认的日报记录",
            "message": "当前没有待确认的日报记录",
            "app_version": APP_VERSION,
        }

    digest_date = normalize_text(pending_delivery.get("digest_date")) or datetime.now(zone).strftime("%Y-%m-%d")
    sent_at = normalize_text(pending_delivery.get("sent_at")) or normalize_text(pending_delivery.get("prepared_at")) or now_text

    if action_name == "confirm":
        _save_daily_state(
            {
                "last_sent_date": digest_date,
                "last_sent_at": sent_at,
                "last_attempt_at": sent_at,
                "last_error": "",
                "app_version": APP_VERSION,
                "last_delivery_fingerprint": normalize_text(pending_delivery.get("message_fingerprint")),
                "last_delivery_message_ids": list(pending_delivery.get("message_ids") or []),
                "pending_delivery": None,
            }
        )
        return {
            **build_delivery_status(),
            "ok": True,
            "message": f"{digest_date} 待确认日报已标记为已发送",
            "action": action_name,
            "app_version": APP_VERSION,
        }

    _save_daily_state(
        {
            "last_error": "",
            "last_attempt_at": now_text,
            "pending_delivery": None,
        }
    )
    return {
        **build_delivery_status(),
        "ok": True,
        "message": f"{digest_date} 待确认状态已清除，后续可重新发送",
        "action": action_name,
        "app_version": APP_VERSION,
    }


def resolve_and_bind_latest_chat() -> dict[str, object]:
    cfg, _, token = _load_runtime_context()
    if not token:
        raise ValueError("Telegram bot token 未配置")

    bot_profile = get_telegram_me(token)
    bot_username = normalize_text(bot_profile.get("username"))
    cfg = save_intel_config({"telegram": {"bot_username": bot_username}})

    resolved = resolve_latest_chat(token)
    if not resolved or not normalize_text(resolved.get("chat_id")):
        return {
            "ok": False,
            "error": f"还没有可识别的会话。请先给 @{bot_username or '机器人'} 发送 /start 或任意消息，再重试。",
            "telegram": cfg.get("telegram"),
            "app_version": APP_VERSION,
        }

    cfg = save_intel_config({"telegram": {**resolved, "bot_username": bot_username}})
    return {
        "ok": True,
        "telegram": cfg.get("telegram"),
        "message": f"已绑定目标会话：{normalize_text(resolved.get('chat_title')) or normalize_text(resolved.get('chat_id'))}",
        "app_version": APP_VERSION,
    }


def send_test_telegram_message() -> dict[str, object]:
    cfg, _, token = _load_runtime_context()
    if not token:
        raise ValueError("Telegram bot token 未配置")

    cfg = _refresh_bot_username(cfg, token)
    cfg = _auto_bind_chat(cfg, token)
    telegram = cfg.get("telegram") if isinstance(cfg.get("telegram"), dict) else {}
    chat_id = normalize_text(telegram.get("chat_id"))
    if not chat_id:
        raise RuntimeError("还没有可用的 Telegram 目标会话。请先给机器人发送 /start，然后点击“识别最近会话”。")

    bot_username = normalize_text(telegram.get("bot_username"))
    chat_title = normalize_text(telegram.get("chat_title")) or chat_id
    timezone_name = _safe_timezone_name(cfg.get("timezone"))
    zone = ZoneInfo(timezone_name)
    today = datetime.now(zone).strftime("%Y-%m-%d")

    payload, delivery_meta = _build_event_pool_payload(
        cfg,
        previous_sent_cutoff_ts=None,
        respect_sent=False,
    )
    message_source = "event_pool_preview"
    payload_date = normalize_text(payload.get("digest_date"))
    payload_message = normalize_text(payload.get("message"))

    if not payload_message:
        raise RuntimeError("测试版日报正文为空，无法发送")

    message_html = "\n".join(
        [
            html.escape("【信息大爆炸 TG 测试日报】"),
            html.escape(f"时间：{format_clock()}"),
            html.escape(f"机器人：@{bot_username or '-'}"),
            html.escape(f"目标：{chat_title}"),
            html.escape(f"正文日期：{payload_date or today}"),
            html.escape("说明：以下为测试版日报正文，不计入正式已发送，不影响每日 08:00 正式推送。"),
            "",
            get_digest_message_html(payload),
        ]
    ).strip()
    send_telegram_message(token, chat_id, message_html, parse_mode="HTML")
    return {
        "ok": True,
        "message": f"测试版日报已发送到 {chat_title}",
        "digest_date": payload_date or today,
        "message_source": message_source,
        "build_stats": payload.get("build_stats"),
        "delivery_meta": delivery_meta,
        "summary": payload.get("summary"),
        "telegram": telegram,
        "app_version": APP_VERSION,
    }


def run_daily_delivery(force: bool = False, now: datetime | None = None) -> dict[str, object]:
    cfg, _, token = _load_runtime_context()
    cfg = _refresh_bot_username(cfg, token)
    cfg = _auto_bind_chat(cfg, token)
    telegram = cfg.get("telegram") if isinstance(cfg.get("telegram"), dict) else {}

    timezone_name = _safe_timezone_name(cfg.get("timezone"))
    zone = ZoneInfo(timezone_name)
    if now is None:
        current = datetime.now(zone)
    elif now.tzinfo is None:
        current = now.replace(tzinfo=zone)
    else:
        current = now.astimezone(zone)

    now_text = current.isoformat(timespec="seconds")
    daily_push_time = normalize_text(cfg.get("daily_push_time")) or DEFAULT_DAILY_PUSH_TIME
    daily_enabled = bool(cfg.get("daily_enabled", True))
    telegram_enabled = bool(telegram.get("enabled", True))
    chat_id = normalize_text(telegram.get("chat_id"))
    chat_title = normalize_text(telegram.get("chat_title")) or chat_id
    today = current.strftime("%Y-%m-%d")
    _, daily_state = _load_daily_state()
    pending_delivery = _normalize_pending_delivery(daily_state.get("pending_delivery"))
    if pending_delivery and normalize_text(pending_delivery.get("digest_date")) not in {"", today}:
        try:
            _save_daily_state({"pending_delivery": None})
        except Exception:
            pass
        pending_delivery = {}

    result = {
        "ok": True,
        "sent": False,
        "app_version": APP_VERSION,
        "checked_at": now_text,
        "last_attempt_at": normalize_text(daily_state.get("last_attempt_at")),
        "last_sent_at": normalize_text(daily_state.get("last_sent_at")),
        "last_error": normalize_text(daily_state.get("last_error")),
        "message": "",
        "daily": {
            "enabled": daily_enabled,
            "push_time": daily_push_time,
            "timezone": timezone_name,
        },
        "telegram": {
            "enabled": telegram_enabled,
            "bot_token_configured": bool(token),
            "bot_username": normalize_text(telegram.get("bot_username")),
            "chat_id": chat_id,
            "chat_title": chat_title,
            "chat_type": normalize_text(telegram.get("chat_type")),
            "chat_ready": bool(token and chat_id),
        },
    }

    if not daily_enabled or not telegram_enabled:
        result["message"] = "每日推送已关闭"
        return result

    if not token:
        result["message"] = "Telegram bot token 未配置"
        return result

    if not chat_id:
        result["message"] = "Telegram 目标 chat 未绑定"
        return result

    due_hour, due_minute = _parse_daily_time(daily_push_time)
    last_sent_date = normalize_text(daily_state.get("last_sent_date"))
    if last_sent_date == today:
        result["message"] = f"今日已推送（{today}）"
        return result

    if pending_delivery and normalize_text(pending_delivery.get("digest_date")) == today:
        result["message"] = _pending_delivery_message(pending_delivery)
        result["pending_delivery"] = pending_delivery
        return result

    if not force and (current.hour, current.minute) < (due_hour, due_minute):
        result["message"] = f"等待下次推送 {today} {daily_push_time} {timezone_name}"
        return result

    send_succeeded = False
    pending_state: dict[str, object] = {}
    try:
        previous_sent_cutoff_ts = _parse_state_timestamp(daily_state.get("last_sent_at"))
        payload, delivery_meta = _build_event_pool_payload(
            cfg,
            previous_sent_cutoff_ts=previous_sent_cutoff_ts,
            respect_sent=True,
        )
        digest_message_html = get_digest_message_html(payload)
        digest_message = normalize_text(payload.get("message"))
        if not digest_message_html and not digest_message:
            raise RuntimeError("正式日报正文为空，无法发送")
        pending_state = {
            "state": "sending",
            "digest_date": normalize_text(payload.get("digest_date")) or today,
            "prepared_at": now_text,
            "sent_at": "",
            "attempt_id": f"daily-{time.time_ns()}",
            "message_fingerprint": _message_fingerprint(digest_message_html or digest_message),
            "message_ids": [],
            "chunk_count": 0,
        }
        _save_daily_state(
            {
                "last_error": "",
                "last_attempt_at": now_text,
                "pending_delivery": pending_state,
            }
        )
        if digest_message_html:
            telegram_results = send_telegram_message(token, chat_id, digest_message_html, parse_mode="HTML")
        else:
            telegram_results = send_telegram_message(token, chat_id, digest_message)
        send_succeeded = True
        pending_state = {
            **pending_state,
            "state": "sent_not_committed",
            "sent_at": now_text,
            "message_ids": _extract_message_ids(telegram_results),
            "chunk_count": len(telegram_results),
        }
        _save_daily_state(
            {
                "last_error": "",
                "last_attempt_at": now_text,
                "pending_delivery": pending_state,
            }
        )
        finalize_digest_payload(payload)
    except Exception as exc:
        if send_succeeded:
            error_text = f"消息已发出，但本地确认失败：{exc}"
            try:
                saved_state = _save_daily_state(
                    {
                        "last_error": error_text,
                        "last_attempt_at": now_text,
                        "pending_delivery": pending_state or None,
                    }
                )
            except Exception:
                saved_state = {
                    "last_error": error_text,
                    "last_attempt_at": now_text,
                }
            result["ok"] = False
            result["last_attempt_at"] = normalize_text(saved_state.get("last_attempt_at"))
            result["last_error"] = normalize_text(saved_state.get("last_error"))
            result["message"] = "日报已发出，但本地确认失败；系统已暂停自动重发"
            result["error"] = str(exc)
            result["pending_delivery"] = pending_state
            return result

        saved_state = _save_daily_state(
            {
                "last_error": str(exc),
                "last_attempt_at": now_text,
                "pending_delivery": None,
            }
        )
        result["ok"] = False
        result["last_attempt_at"] = normalize_text(saved_state.get("last_attempt_at"))
        result["last_error"] = normalize_text(saved_state.get("last_error"))
        result["message"] = "日报发送失败"
        result["error"] = str(exc)
        return result

    saved_state = _save_daily_state(
        {
            "last_sent_date": today,
            "last_sent_at": now_text,
            "last_error": "",
            "last_attempt_at": now_text,
            "app_version": APP_VERSION,
            "last_delivery_fingerprint": normalize_text(pending_state.get("message_fingerprint")),
            "last_delivery_message_ids": list(pending_state.get("message_ids") or []),
            "pending_delivery": None,
        }
    )
    result["sent"] = True
    result["last_attempt_at"] = normalize_text(saved_state.get("last_attempt_at"))
    result["last_sent_at"] = normalize_text(saved_state.get("last_sent_at"))
    result["last_error"] = ""
    result["message"] = "日报发送成功"
    if isinstance(delivery_meta, dict):
        result["delivery_meta"] = delivery_meta
    return result
