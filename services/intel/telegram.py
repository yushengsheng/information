from __future__ import annotations

import json
from typing import Any
from urllib import error, request


def _telegram_api_call(bot_token: str, method: str, payload: dict[str, object] | None = None, timeout: int = 20) -> dict[str, Any]:
    token = str(bot_token or "").strip()
    if not token:
        raise ValueError("Telegram bot token 未配置")

    url = f"https://api.telegram.org/bot{token}/{method}"
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=body, headers=headers, method="POST" if payload is not None else "GET")
    try:
        with request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {}
        description = data.get("description") if isinstance(data, dict) else ""
        raise RuntimeError(str(description or exc)) from exc

    data = json.loads(raw)
    if not data.get("ok"):
        description = data.get("description") or "unknown telegram api error"
        raise RuntimeError(str(description))
    if not isinstance(data, dict):
        raise RuntimeError("telegram api 返回异常响应")
    return data


def get_telegram_me(bot_token: str) -> dict[str, object]:
    data = _telegram_api_call(bot_token, "getMe")
    result = data.get("result")
    if isinstance(result, dict):
        return result
    raise RuntimeError("telegram getMe 返回异常")


def get_telegram_updates(bot_token: str) -> list[dict[str, object]]:
    try:
        data = _telegram_api_call(bot_token, "getUpdates")
    except RuntimeError as exc:
        if "conflict" not in str(exc).lower():
            raise
        _telegram_api_call(bot_token, "deleteWebhook", {"drop_pending_updates": False})
        data = _telegram_api_call(bot_token, "getUpdates")
    result = data.get("result")
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    return []


def resolve_latest_chat(bot_token: str) -> dict[str, object] | None:
    updates = get_telegram_updates(bot_token)
    for update in reversed(updates):
        for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
            node = update.get(key)
            if not isinstance(node, dict):
                continue
            chat = node.get("chat")
            if not isinstance(chat, dict):
                continue
            title = str(chat.get("title") or chat.get("username") or chat.get("first_name") or chat.get("id") or "").strip()
            return {
                "chat_id": str(chat.get("id") or "").strip(),
                "chat_type": str(chat.get("type") or "").strip(),
                "chat_title": title,
            }
    return None


def split_telegram_message(text: str, limit: int = 3500) -> list[str]:
    value = str(text or "").strip()
    if not value:
        return []
    if len(value) <= limit:
        return [value]

    chunks: list[str] = []
    remaining = value
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < limit * 0.5:
            split_at = limit
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def send_telegram_message(bot_token: str, chat_id: str, text: str, parse_mode: str | None = None) -> list[dict[str, object]]:
    target = str(chat_id or "").strip()
    if not target:
        raise ValueError("Telegram chat_id 未配置")

    results: list[dict[str, object]] = []
    for chunk in split_telegram_message(text):
        payload = {
            "chat_id": target,
            "text": chunk,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = str(parse_mode)
        data = _telegram_api_call(
            bot_token,
            "sendMessage",
            payload,
            timeout=25,
        )
        result = data.get("result")
        if isinstance(result, dict):
            results.append(result)
    return results
