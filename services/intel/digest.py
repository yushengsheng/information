from __future__ import annotations

from collections import Counter
import html
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app_config import APP_VERSION
from monitor import format_clock
from services.intel.ai import apply_digest_summaries
from services.intel.collectors import collect_custom_user_items, collect_topic_items
from services.intel.event_pool import build_event_pool_entries, build_topic_items_from_event_pool
from services.intel.opencli import OPENCLI_CACHE_TTL_SECONDS
from services.intel.ranking import (
    apply_sent_filter,
    dedupe_items,
    rank_items,
    select_hot_items,
    select_items_with_x_priority,
    select_persistent_items,
    select_world_items,
)
from services.intel.store import load_intel_config, load_sent_registry, save_latest_digest, save_latest_sent_digest, save_sent_registry
from services.intel.store import load_event_pool_since, load_snapshot_pool_since, save_event_pool_entries, save_snapshot_pool_entry
from services.intel.text import item_dedupe_key, normalize_text, normalize_tweet_text, short_summary_text

_DIGEST_ORIGINAL_LINK_PATTERN = re.compile(r"^原文：(https?://\S+)$")
_DIGEST_NUMBER_MARKERS = {
    1: "❶",
    2: "❷",
    3: "❸",
    4: "❹",
    5: "❺",
    6: "❻",
    7: "❼",
    8: "❽",
    9: "❾",
    10: "❿",
    11: "⓫",
    12: "⓬",
    13: "⓭",
    14: "⓮",
    15: "⓯",
    16: "⓰",
    17: "⓱",
    18: "⓲",
    19: "⓳",
    20: "⓴",
    21: "㉑",
    22: "㉒",
    23: "㉓",
    24: "㉔",
    25: "㉕",
    26: "㉖",
    27: "㉗",
    28: "㉘",
    29: "㉙",
    30: "㉚",
}


def _build_source_counts(items: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        source = normalize_text(item.get("source")).lower() or "unknown"
        counts[source] = counts.get(source, 0) + 1
    return counts


def _source_role_label(item: dict[str, object]) -> str:
    source_role = normalize_text(item.get("source_role")).lower()
    if source_role == "confirmation":
        return "确认源"
    if source_role == "community":
        return "社区补充"
    if source_role == "discovery":
        return "发现源"
    return "来源"


def _item_reason_chips(item: dict[str, object]) -> list[str]:
    chips: list[str] = []
    event_label = normalize_text(item.get("topic_event_label"))
    if event_label:
        chips.append(f"事件 {event_label}")

    confirmation_count = max(int(item.get("event_confirmation_count", 0) or 0), 0)
    if confirmation_count > 0:
        chips.append(f"已确认 {confirmation_count}")

    event_source_count = max(
        int(
            item.get("event_source_count")
            or item.get("cluster_source_count")
            or 0
        ),
        0,
    )
    if event_source_count > 1:
        chips.append(f"跨源 {event_source_count}")

    authority_score = max(
        float(item.get("event_authority_score", item.get("source_authority_score", 0.0)) or 0.0),
        0.0,
    )
    if authority_score >= 1.0:
        chips.append(f"权威 {authority_score:.1f}")

    window_hits = max(int(item.get("window_hits", 0) or 0), 0)
    if window_hits > 1:
        chips.append(f"持续 {window_hits} 轮")

    chips.append(_source_role_label(item))
    return chips[:6]


def _annotate_selected_sections(sections: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    annotated: dict[str, list[dict[str, object]]] = {}
    for section, rows in sections.items():
        section_items: list[dict[str, object]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            reason_chips = _item_reason_chips(item)
            item["selection_reason_chips"] = reason_chips
            item["selection_reason_text"] = "；".join(reason_chips) if reason_chips else "常规优先级"
            item["source_role_label"] = _source_role_label(item)
            section_items.append(item)
        annotated[section] = section_items
    return annotated


def _build_lane_diagnostics(items: list[dict[str, object]]) -> dict[str, object]:
    source_counter: Counter[str] = Counter()
    event_counter: Counter[str] = Counter()
    event_matched = 0
    confirmed = 0
    authority_scores: list[float] = []

    for item in items:
        source_counter[normalize_text(item.get("source")).lower() or "unknown"] += 1
        event_label = normalize_text(item.get("topic_event_label"))
        if event_label:
            event_counter[event_label] += 1
            event_matched += 1
        if int(item.get("event_confirmation_count", 0) or 0) >= 1:
            confirmed += 1
        authority = float(item.get("event_authority_score", item.get("source_authority_score", 0.0)) or 0.0)
        if authority > 0:
            authority_scores.append(authority)

    top_events = [
        {"label": label, "count": count}
        for label, count in event_counter.most_common(3)
    ]
    avg_authority = round(sum(authority_scores) / len(authority_scores), 2) if authority_scores else 0.0

    return {
        "total": len(items),
        "event_matched": event_matched,
        "confirmed": confirmed,
        "source_counts": dict(source_counter),
        "top_events": top_events,
        "avg_authority": avg_authority,
    }


def _build_selection_diagnostics(sections: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    lane_stats: dict[str, dict[str, object]] = {}
    all_items: list[dict[str, object]] = []
    for lane in ("crypto", "world", "persistent", "hot", "custom"):
        lane_items = [dict(item) for item in sections.get(lane, []) if isinstance(item, dict)]
        lane_stats[lane] = _build_lane_diagnostics(lane_items)
        all_items.extend(lane_items)
    return {
        "overall": _build_lane_diagnostics(all_items),
        "lanes": lane_stats,
    }


def _item_summary_text(item: dict[str, object], max_len: int) -> str:
    summary_text = normalize_text(item.get("summary_text"))
    if summary_text:
        return short_summary_text(summary_text, max_len=max_len)
    return short_summary_text(normalize_tweet_text(item.get("text")), max_len=max_len)


def _exclude_seen_items(items: list[dict[str, object]], seen_items: list[dict[str, object]]) -> list[dict[str, object]]:
    seen_keys = {item_dedupe_key(item) for item in seen_items if isinstance(item, dict)}
    if not seen_keys:
        return list(items)
    return [item for item in items if item_dedupe_key(item) not in seen_keys]


def _section_title(index: int, title: str) -> str:
    numerals = {1: "一", 2: "二", 3: "三", 4: "四", 5: "五"}
    prefix = numerals.get(index, str(index))
    return f"{prefix}、{title}"


def _digest_item_marker(index: int) -> str:
    return _DIGEST_NUMBER_MARKERS.get(index, f"[{index}]")


def _digest_original_link_html(url: str) -> str:
    normalized_url = normalize_text(url)
    if not normalized_url or normalized_url == "-":
        return html.escape(f"原文：{normalized_url or '-'}")
    return f'<a href="{html.escape(normalized_url, quote=True)}">原文</a>'


def build_digest_message_html_from_text(message: object) -> str:
    raw_message = str(message or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw_message.strip():
        return ""

    html_lines: list[str] = []
    for line in raw_message.split("\n"):
        match = _DIGEST_ORIGINAL_LINK_PATTERN.fullmatch(line.strip())
        if match:
            html_lines.append(_digest_original_link_html(match.group(1)))
        else:
            html_lines.append(html.escape(line))
    return "\n".join(html_lines).strip()


def get_digest_message_html(payload: dict[str, object]) -> str:
    message_html = str(payload.get("message_html") or "").strip()
    if message_html:
        return message_html
    return build_digest_message_html_from_text(payload.get("message"))


def finalize_digest_payload(payload: dict[str, object]) -> dict[str, object]:
    sent_registry = load_sent_registry()
    all_sent_keys = sent_registry.get("keys") if isinstance(sent_registry.get("keys"), dict) else {}
    now_value = int(time.time())
    sections = payload.get("sections") if isinstance(payload.get("sections"), dict) else {}

    merged_sent_keys = dict(all_sent_keys)
    for section_items in sections.values():
        if not isinstance(section_items, list):
            continue
        for item in section_items:
            if isinstance(item, dict):
                merged_sent_keys[item_dedupe_key(item)] = now_value

    sent_registry["keys"] = merged_sent_keys
    sent_registry["updated_at"] = now_value
    save_sent_registry(sent_registry)

    final_payload = dict(payload)
    final_payload["final"] = True
    save_latest_sent_digest(final_payload)
    return final_payload


def _empty_topic_sections() -> dict[str, list[dict[str, object]]]:
    return {"crypto": [], "world": [], "persistent": [], "hot": [], "custom": []}


def _extract_digest_limits(cfg: dict[str, object]) -> tuple[int, int, int, int]:
    limits = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}
    crypto_limit = int(limits.get("crypto", 10)) if isinstance(limits, dict) else 10
    world_limit = int(limits.get("world", 3)) if isinstance(limits, dict) else 3
    hot_limit = int(limits.get("hot", 2)) if isinstance(limits, dict) else 2
    custom_limit = int(limits.get("custom_user", 3)) if isinstance(limits, dict) else 3
    return crypto_limit, world_limit, hot_limit, custom_limit


def collect_digest_candidates(cfg: dict[str, object] | None = None) -> dict[str, list[dict[str, object]]]:
    config = cfg if isinstance(cfg, dict) else load_intel_config()
    crypto_limit, world_limit, hot_limit, custom_limit = _extract_digest_limits(config)
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {
            executor.submit(collect_topic_items, config, "crypto", crypto_limit): "crypto",
            executor.submit(collect_topic_items, config, "world", world_limit): "world",
            executor.submit(collect_custom_user_items, config, custom_limit): "custom",
        }
        if hot_limit > 0:
            future_map[executor.submit(collect_topic_items, config, "hot", hot_limit)] = "hot"
        collected = _empty_topic_sections()
        for future in as_completed(future_map):
            collected[future_map[future]] = future.result()
    return collected


def _copy_candidate_pool(collected: dict[str, list[dict[str, object]]]) -> dict[str, list[dict[str, object]]]:
    normalized = _empty_topic_sections()
    for topic in normalized:
        rows = collected.get(topic) if isinstance(collected, dict) else []
        if isinstance(rows, list):
            normalized[topic] = [dict(row) for row in rows if isinstance(row, dict)]
    return normalized


def _aggregate_window_topic_items(items: list[dict[str, object]]) -> list[dict[str, object]]:
    aggregated: dict[str, dict[str, object]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        record = dict(item)
        dedupe_key = item_dedupe_key(record)
        try:
            seen_at = int(record.get("fetched_at") or time.time())
        except Exception:
            seen_at = int(time.time())
        existing = aggregated.get(dedupe_key)
        if existing is None:
            record["window_hits"] = 1
            record["window_first_seen_at"] = seen_at
            record["window_last_seen_at"] = seen_at
            record["window_span_hours"] = 0.0
            aggregated[dedupe_key] = record
            continue

        existing["window_hits"] = int(existing.get("window_hits", 1) or 1) + 1
        existing["window_first_seen_at"] = min(int(existing.get("window_first_seen_at") or seen_at), seen_at)
        existing["window_last_seen_at"] = max(int(existing.get("window_last_seen_at") or seen_at), seen_at)
        existing["window_span_hours"] = round(
            max(int(existing.get("window_last_seen_at") or seen_at) - int(existing.get("window_first_seen_at") or seen_at), 0) / 3600.0,
            2,
        )

        if len(normalize_tweet_text(record.get("text"))) > len(normalize_tweet_text(existing.get("text"))):
            existing["text"] = record.get("text")
        if not normalize_text(existing.get("url")) and normalize_text(record.get("url")):
            existing["url"] = record.get("url")
        if not normalize_text(existing.get("author")) and normalize_text(record.get("author")):
            existing["author"] = record.get("author")
        if not normalize_text(existing.get("created_at")) and normalize_text(record.get("created_at")):
            existing["created_at"] = record.get("created_at")
        try:
            existing["score"] = max(float(existing.get("score", 0) or 0), float(record.get("score", 0) or 0))
        except Exception:
            pass
        try:
            existing["topic_quality_score"] = max(
                float(existing.get("topic_quality_score", 0) or 0),
                float(record.get("topic_quality_score", 0) or 0),
            )
        except Exception:
            pass
        try:
            existing["topic_match_count"] = max(
                int(existing.get("topic_match_count", 0) or 0),
                int(record.get("topic_match_count", 0) or 0),
            )
        except Exception:
            pass
        try:
            existing["topic_news_match_count"] = max(
                int(existing.get("topic_news_match_count", 0) or 0),
                int(record.get("topic_news_match_count", 0) or 0),
            )
        except Exception:
            pass
        existing["fetched_at"] = max(int(existing.get("fetched_at") or seen_at), seen_at)
    return list(aggregated.values())


def build_aggregate_candidate_pool(
    live_candidates: dict[str, list[dict[str, object]]],
    *,
    since_ts: int,
) -> tuple[dict[str, list[dict[str, object]]], dict[str, object]]:
    snapshot_entries = load_snapshot_pool_since(since_ts)
    merged = _empty_topic_sections()
    for entry in snapshot_entries:
        topics = entry.get("topics") if isinstance(entry.get("topics"), dict) else {}
        for topic in merged:
            topic_items = topics.get(topic) if isinstance(topics.get(topic), list) else []
            merged[topic].extend(dict(row) for row in topic_items if isinstance(row, dict))
    for topic in merged:
        merged[topic].extend(dict(row) for row in live_candidates.get(topic, []) if isinstance(row, dict))
        merged[topic] = _aggregate_window_topic_items(merged[topic])
    return merged, {
        "snapshot_count": len(snapshot_entries),
        "since_ts": int(since_ts),
    }


def build_event_pool_candidate_pool(
    live_candidates: dict[str, list[dict[str, object]]],
    *,
    since_ts: int,
    generated_at: str = "",
    digest_date: str = "",
) -> tuple[dict[str, list[dict[str, object]]], dict[str, object]]:
    live_entries = build_event_pool_entries(
        live_candidates,
        generated_at=generated_at,
        digest_date=digest_date,
    )
    stored_event_entries = load_event_pool_since(since_ts)
    bootstrap_snapshot_count = 0
    if not stored_event_entries:
        snapshot_entries = load_snapshot_pool_since(since_ts)
        bootstrap_snapshot_count = len(snapshot_entries)
        for snapshot in snapshot_entries:
            topics = snapshot.get("topics") if isinstance(snapshot.get("topics"), dict) else {}
            stored_event_entries.extend(
                build_event_pool_entries(
                    topics,
                    captured_at=int(snapshot.get("captured_at") or 0) or None,
                    generated_at=str(snapshot.get("generated_at") or ""),
                    digest_date=str(snapshot.get("digest_date") or ""),
                )
            )

    event_entries = stored_event_entries + live_entries
    topics = build_topic_items_from_event_pool(event_entries)
    return topics, {
        "event_pool_entry_count": len(event_entries),
        "event_count": sum(len(rows) for rows in topics.values()),
        "bootstrap_snapshot_count": bootstrap_snapshot_count,
        "since_ts": int(since_ts),
    }


def refresh_latest_digest_snapshot() -> dict[str, object]:
    cfg = load_intel_config()
    collected = collect_digest_candidates(cfg)
    payload = build_digest_payload(final=False, respect_sent=False, cfg=cfg, collected=collected)
    payload["snapshot_mode"] = "background"
    save_latest_digest(payload, persist_history=False)
    save_snapshot_pool_entry(
        _copy_candidate_pool(collected),
        generated_at=str(payload.get("generated_at") or ""),
        digest_date=str(payload.get("digest_date") or ""),
    )
    save_event_pool_entries(
        build_event_pool_entries(
            collected,
            generated_at=str(payload.get("generated_at") or ""),
            digest_date=str(payload.get("digest_date") or ""),
        )
    )
    return payload


def build_digest_payload(
    final: bool = False,
    respect_sent: Optional[bool] = None,
    sent_cutoff_ts: int | None = None,
    *,
    cfg: dict[str, object] | None = None,
    collected: dict[str, list[dict[str, object]]] | None = None,
    window_meta: dict[str, object] | None = None,
) -> dict[str, object]:
    build_started = time.perf_counter()
    cfg = cfg if isinstance(cfg, dict) else load_intel_config()
    if respect_sent is None:
        respect_sent = final

    crypto_limit, world_limit, hot_limit, custom_limit = _extract_digest_limits(cfg)
    persistent_limit = 3

    sent_registry = load_sent_registry()
    all_sent_keys = sent_registry.get("keys") if isinstance(sent_registry.get("keys"), dict) else {}
    if respect_sent:
        if sent_cutoff_ts is None:
            sent_keys = dict(all_sent_keys)
        else:
            sent_keys = {
                str(key): value
                for key, value in all_sent_keys.items()
                if isinstance(value, (int, float)) and int(value) <= sent_cutoff_ts
            }
    else:
        sent_keys = {}

    collect_started = time.perf_counter()
    collected = _copy_candidate_pool(collected) if isinstance(collected, dict) else collect_digest_candidates(cfg)

    crypto_items = rank_items(dedupe_items(collected["crypto"]))
    world_items = rank_items(dedupe_items(collected["world"]))
    hot_items = rank_items(dedupe_items(collected["hot"]))
    custom_items = rank_items(dedupe_items(collected["custom"]))
    collect_finished = time.perf_counter()

    raw_counts = {
        "crypto": len(crypto_items),
        "world": len(world_items),
        "persistent": 0,
        "hot": len(hot_items),
        "custom": len(custom_items),
    }

    crypto_filtered = apply_sent_filter(crypto_items, sent_keys)
    world_filtered = apply_sent_filter(world_items, sent_keys)
    hot_filtered = apply_sent_filter(hot_items, sent_keys)
    custom_filtered = apply_sent_filter(custom_items, sent_keys)

    crypto_selected = select_items_with_x_priority(crypto_filtered, crypto_limit)
    world_selected = select_world_items(world_filtered, world_limit)
    persistent_candidates = rank_items(
        dedupe_items(
            _exclude_seen_items(
                crypto_filtered + world_filtered,
                crypto_selected + world_selected,
            )
        )
    )
    raw_counts["persistent"] = len(persistent_candidates)
    persistent_selected = select_persistent_items(persistent_candidates, persistent_limit)
    hot_filtered = _exclude_seen_items(hot_filtered, crypto_selected + world_selected + persistent_selected)
    hot_selected = select_hot_items(hot_filtered, hot_limit)
    custom_filtered = _exclude_seen_items(custom_filtered, crypto_selected + world_selected + persistent_selected + hot_selected)

    custom_grouped: dict[str, list[dict[str, object]]] = {}
    for item in custom_filtered:
        owner = normalize_text(item.get("owner") or item.get("author") or "unknown")
        custom_grouped.setdefault(owner, []).append(item)

    custom_selected: list[dict[str, object]] = []
    for group in custom_grouped.values():
        custom_selected.extend(group[:custom_limit])

    sections = {
        "crypto": crypto_selected,
        "world": world_selected,
        "persistent": persistent_selected,
        "hot": hot_selected,
        "custom": custom_selected,
    }
    sections = _annotate_selected_sections(sections)
    selection_diagnostics = _build_selection_diagnostics(sections)
    unsent_counts = {
        "crypto": len(crypto_filtered),
        "world": len(world_filtered),
        "persistent": len(persistent_candidates),
        "hot": len(hot_filtered),
        "custom": sum(len(group) for group in custom_grouped.values()),
    }

    summary_started = time.perf_counter()
    summary_meta = apply_digest_summaries(sections, cfg)
    summary_finished = time.perf_counter()

    timezone_name = normalize_text(cfg.get("timezone")) or "Asia/Shanghai"
    try:
        now_value = datetime.now(ZoneInfo(timezone_name))
    except ZoneInfoNotFoundError:
        timezone_name = "Asia/Shanghai"
        now_value = datetime.now(ZoneInfo(timezone_name))
    date_text = now_value.strftime("%Y-%m-%d")
    daily_push_time = normalize_text(cfg.get("daily_push_time")) or "08:00"
    lines = [f"【信息大爆炸日报 | {date_text} {daily_push_time}】", ""]
    html_lines = [html.escape(lines[0]), ""]

    def append_digest_entry(index: int, tag: str, text: str, url: str) -> None:
        content = f"{_digest_item_marker(index)} {tag} {text}".strip()
        lines.append(content)
        html_lines.append(html.escape(content))
        lines.append(f"原文：{url}")
        html_lines.append(_digest_original_link_html(url))

    def add_section(title: str, section_items: list[dict[str, object]], max_n: int) -> None:
        lines.append(f"{title}（{len(section_items)}条）")
        html_lines.append(html.escape(lines[-1]))
        if not section_items:
            lines.append("- 今日无新增高价值信息")
            html_lines.append(html.escape(lines[-1]))
            lines.append("")
            html_lines.append("")
            return
        for index, item in enumerate(section_items[:max_n], 1):
            if index > 1:
                lines.append("")
                html_lines.append("")
            text = _item_summary_text(item, max_len=110)
            url = normalize_text(item.get("url")) or "-"
            source = normalize_text(item.get("source")).upper() or "SRC"
            author = normalize_text(item.get("author"))
            tag = f"[{source}]" + (f" @{author}" if author else "")
            append_digest_entry(index, tag, text, url)
        lines.append("")
        html_lines.append("")

    add_section(_section_title(1, "币圈/加密"), crypto_selected, crypto_limit)
    add_section(_section_title(2, "世界大事件"), world_selected, world_limit)
    next_section_index = 3
    if persistent_selected:
        add_section(_section_title(next_section_index, "持续发酵"), persistent_selected, persistent_limit)
        next_section_index += 1
    if hot_limit > 0 or hot_selected:
        add_section(_section_title(next_section_index, "热门补充"), hot_selected, hot_limit)
        next_section_index += 1

    if custom_selected:
        lines.append(f"{_section_title(next_section_index, '自定义关注账号')}（{len(custom_selected)}条）")
        html_lines.append(html.escape(lines[-1]))
        index = 1
        for owner, group in custom_grouped.items():
            for item in group[:custom_limit]:
                if index > 1:
                    lines.append("")
                    html_lines.append("")
                text = _item_summary_text(item, max_len=100)
                url = normalize_text(item.get("url")) or "-"
                append_digest_entry(index, f"[X] @{owner}", text, url)
                index += 1
        lines.append("")
        html_lines.append("")

    summary_note = "摘要优先使用 AI 生成；不可用时自动回退到翻译 + 截断。"
    lines.append(f"说明：主线优先跟踪币圈/加密与世界大事，“持续发酵”专门保留跨时间窗口仍在升温的重要事件，热门补充仅少量加入其他高热度事件；已去重；X 为主源，Reddit/RSS 为补充；{summary_note} 点击原文可追溯来源。")
    html_lines.append(html.escape(lines[-1]))
    payload = {
        "ok": True,
        "app_version": APP_VERSION,
        "digest_date": date_text,
        "generated_at": format_clock(),
        "config": cfg,
        "sections": sections,
        "message": "\n".join(lines).strip(),
        "message_html": "\n".join(html_lines).strip(),
        "summary": summary_meta,
        "counts": {
            "crypto": len(crypto_selected),
            "world": len(world_selected),
            "persistent": len(persistent_selected),
            "hot": len(hot_selected),
            "custom": len(custom_selected),
        },
        "build_stats": {
            "cache_ttl_seconds": OPENCLI_CACHE_TTL_SECONDS,
            "raw_counts": raw_counts,
            "unsent_counts": unsent_counts,
            "selected_counts": {
                "crypto": len(crypto_selected),
                "world": len(world_selected),
                "persistent": len(persistent_selected),
                "hot": len(hot_selected),
                "custom": len(custom_selected),
            },
            "source_counts": {
                "crypto": _build_source_counts(crypto_items),
                "world": _build_source_counts(world_items),
                "persistent": _build_source_counts(persistent_candidates),
                "hot": _build_source_counts(hot_items),
                "custom": _build_source_counts(custom_items),
            },
            "selection_diagnostics": selection_diagnostics,
            "summary": summary_meta,
            "timings_ms": {
                "collect": round((collect_finished - collect_started) * 1000),
                "summary": round((summary_finished - summary_started) * 1000),
                "total": round((time.perf_counter() - build_started) * 1000),
            },
        },
        "final": final,
    }
    if isinstance(window_meta, dict) and window_meta:
        payload["build_stats"]["window"] = dict(window_meta)

    if final:
        return finalize_digest_payload(payload)

    return payload
