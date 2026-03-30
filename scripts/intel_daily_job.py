#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.intel.delivery import (
    build_delivery_status,
    resolve_and_bind_latest_chat,
    run_daily_delivery,
    send_test_telegram_message,
)
from services.intel.store import load_intel_secrets
from services.intel.telegram import get_telegram_updates
from services.intel.text import normalize_text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="信息大爆炸 Telegram / 日报运维入口")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="查看 Telegram 与日报投递状态")
    subparsers.add_parser("resolve-chat", help="识别并绑定最近的 Telegram 会话")
    subparsers.add_parser("test-send", help="发送一条 Telegram 测试消息")

    run_daily = subparsers.add_parser("run-daily", help="执行一次日报投递检查")
    run_daily.add_argument("--force", action="store_true", help="忽略时间窗口，立即尝试发送日报")
    return parser


def extract_latest_chat(updates: list[dict[str, object]]) -> dict[str, object]:
    for update in reversed(updates):
        for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
            node = update.get(key)
            if not isinstance(node, dict):
                continue
            chat = node.get("chat")
            if not isinstance(chat, dict):
                continue
            return {
                "chat_id": normalize_text(chat.get("id")),
                "chat_type": normalize_text(chat.get("type")),
                "chat_title": normalize_text(chat.get("title") or chat.get("username") or chat.get("first_name") or chat.get("id")),
            }
    return {}


def command_status() -> tuple[dict[str, object], int]:
    payload = build_delivery_status()
    secrets = load_intel_secrets()
    token = normalize_text(secrets.get("telegram_bot_token"))
    debug: dict[str, object] = {}
    if token:
        try:
            updates = get_telegram_updates(token)
            debug["updates_count"] = len(updates)
            debug["latest_chat"] = extract_latest_chat(updates)
        except Exception as exc:
            debug["telegram_api_error"] = str(exc)
    payload["debug"] = debug
    return payload, 0


def command_resolve_chat() -> tuple[dict[str, object], int]:
    try:
        payload = resolve_and_bind_latest_chat()
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}, 1
    return payload, 0 if payload.get("ok") else 1


def command_test_send() -> tuple[dict[str, object], int]:
    try:
        payload = send_test_telegram_message()
        return payload, 0
    except (ValueError, RuntimeError) as exc:
        return {"ok": False, "error": str(exc)}, 1


def command_run_daily(force: bool) -> tuple[dict[str, object], int]:
    payload = run_daily_delivery(force=force)
    if payload.get("ok", True):
        return payload, 0
    return payload, 1


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "status":
        payload, code = command_status()
    elif args.command == "resolve-chat":
        payload, code = command_resolve_chat()
    elif args.command == "test-send":
        payload, code = command_test_send()
    else:
        payload, code = command_run_daily(force=bool(getattr(args, "force", False)))

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
