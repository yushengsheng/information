#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from services.intel.digest import build_digest_payload
from services.intel.store import load_delivery_state, load_intel_config, load_intel_secrets
from services.intel.telegram import send_telegram_message
from services.intel.text import normalize_text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="单次补发今日日报到 Telegram，不计入正式推送状态。")
    parser.add_argument("--delay-seconds", type=int, default=0, help="延迟多少秒后执行")
    parser.add_argument("--label", default="测试补发", help="附加在消息头部的标记")
    parser.add_argument(
        "--respect-last-sent",
        action="store_true",
        help="按最近一次正式发送去重；默认关闭，便于测试链路时看到完整当下日报。",
    )
    return parser.parse_args()


def parse_iso_timestamp(value: object) -> int | None:
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


def main() -> int:
    args = parse_args()
    if args.delay_seconds > 0:
        time.sleep(args.delay_seconds)

    cfg = load_intel_config()
    telegram = cfg.get("telegram") if isinstance(cfg.get("telegram"), dict) else {}
    secrets = load_intel_secrets()
    delivery_state = load_delivery_state()
    daily_state = delivery_state.get("daily_digest") if isinstance(delivery_state.get("daily_digest"), dict) else {}

    token = normalize_text(secrets.get("telegram_bot_token"))
    chat_id = normalize_text(telegram.get("chat_id"))
    if not token:
        raise RuntimeError("Telegram bot token 未配置")
    if not chat_id:
        raise RuntimeError("Telegram chat_id 未配置")

    sent_cutoff_ts = parse_iso_timestamp(daily_state.get("last_sent_at")) if args.respect_last_sent else None
    payload = build_digest_payload(
        final=False,
        respect_sent=bool(args.respect_last_sent),
        sent_cutoff_ts=sent_cutoff_ts,
    )

    now_text = datetime.now().astimezone().isoformat(timespec="seconds")
    prefix = "\n".join(
        [
            f"【信息大爆炸日报{normalize_text(args.label) or '测试补发'}】",
            f"发送时间：{now_text}",
            "说明：这是一条单次验证发送，不计入正式推送状态，也不会改动明天的正式日报基线。",
            "",
        ]
    )
    message = f"{prefix}{normalize_text(payload.get('message'))}".strip()
    send_telegram_message(token, chat_id, message)

    result = {
        "ok": True,
        "sent_at": now_text,
        "chat_id": chat_id,
        "selected_counts": payload.get("build_stats", {}).get("selected_counts", {}),
        "sent_cutoff_ts": sent_cutoff_ts,
        "respect_last_sent": bool(args.respect_last_sent),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
