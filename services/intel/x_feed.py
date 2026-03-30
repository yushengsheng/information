from __future__ import annotations

from services.intel.collectors import collect_x_items, enrich_items_with_thread
from services.intel.opencli import run_opencli_json_command
from services.intel.text import (
    is_low_signal_text,
    normalize_text,
    normalize_tweet_text,
    parse_x_limit,
    parse_x_username,
    translate_items_for_display,
)


def build_x_feed_payload(payload: dict[str, object]) -> tuple[dict[str, object], int]:
    username = parse_x_username(payload.get("username"))
    keyword = normalize_text(payload.get("keyword"))
    limit = parse_x_limit(payload.get("limit", 10))

    query_parts = [f"from:{username}", "-filter:replies"]
    if keyword:
        query_parts.append(keyword)
    query = " ".join(query_parts)

    fetch_limit = min(max(limit * 3, 12), 36)
    command = ["twitter", "search", query, "--limit", str(fetch_limit), "-f", "json"]
    try:
        parsed, _ = run_opencli_json_command(command, timeout=90)
    except Exception as exc:
        return (
            {
                "ok": False,
                "error": "opencli 执行失败",
                "detail": str(exc) or "unknown error",
                "command": " ".join(command),
            },
            400,
        )
    items = collect_x_items(parsed)
    items = enrich_items_with_thread(items, max_enrich=min(4, limit, fetch_limit))

    if keyword:
        keyword_lower = keyword.lower()
        items = [item for item in items if keyword_lower in str(item.get("text", "")).lower()]

    high_signal: list[dict[str, object]] = []
    low_signal: list[dict[str, object]] = []
    for item in items:
        text = normalize_tweet_text(item.get("text"))
        item["text"] = text
        if is_low_signal_text(text):
            low_signal.append(item)
        else:
            high_signal.append(item)

    selected_items = high_signal[:limit] if high_signal else low_signal[:limit]
    for item in selected_items:
        text = normalize_tweet_text(item.get("text"))
        item["text"] = text or "（未获取到正文，可点击原帖查看）"

    translate_items_for_display(selected_items)

    return (
        {
            "ok": True,
            "username": username,
            "keyword": keyword,
            "query": query,
            "count": len(selected_items),
            "high_signal_count": len(high_signal),
            "low_signal_count": len(low_signal),
            "items": selected_items,
            "raw": parsed,
        },
        200,
    )
