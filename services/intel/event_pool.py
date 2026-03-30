from __future__ import annotations

import time

from services.intel.event_identity import build_group_event_identity, build_item_event_identity
from services.intel.ranking import dedupe_items, extract_event_tokens, jaccard_similarity, rank_items
from services.intel.text import normalize_searchable_text, normalize_text, normalize_tweet_text, parse_created_at_to_ts
from services.intel.topics import classify_source_reliability, classify_topic_event, get_topic_rule


def _event_signature(topic: str, items: list[dict[str, object]]) -> tuple[str, list[str]]:
    return build_group_event_identity(topic, items)


def _choose_representatives(items: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    representatives: dict[str, dict[str, object]] = {}
    for item in sorted(
        items,
        key=lambda row: (
            float(row.get("selection_score", 0.0) or 0.0),
            float(row.get("topic_quality_score", 0.0) or 0.0),
            float(row.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    ):
        source = normalize_text(item.get("source")).lower()
        if source not in {"x", "rss", "reddit"} or source in representatives:
            continue
        representatives[source] = dict(item)
    return representatives


def _hydrate_representative(topic: str, representative: dict[str, object]) -> dict[str, object]:
    item = dict(representative)
    rule = get_topic_rule(topic)
    searchable = normalize_searchable_text(item.get("text"), item.get("url"), item.get("subreddit"))
    if not normalize_text(item.get("topic_event_type")):
        item.update(classify_topic_event(rule, searchable))
    if not normalize_text(item.get("source_role")):
        item.update(
            classify_source_reliability(
                item.get("source"),
                source_domain=item.get("source_domain"),
                external_domain=item.get("external_domain"),
                subreddit=item.get("subreddit"),
                author=item.get("author"),
            )
        )
    if not normalize_text(item.get("event_identity_key")):
        item.update(build_item_event_identity(item))
    return item


def build_event_pool_entries(
    collected: dict[str, list[dict[str, object]]],
    *,
    captured_at: int | None = None,
    generated_at: str = "",
    digest_date: str = "",
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    snapshot_at = int(captured_at or time.time())
    for topic in ("crypto", "world", "hot", "custom"):
        topic_items = collected.get(topic) if isinstance(collected, dict) else []
        if not isinstance(topic_items, list) or not topic_items:
            continue
        ranked_items = rank_items(dedupe_items([dict(item) for item in topic_items if isinstance(item, dict)]))
        clusters: dict[str, list[dict[str, object]]] = {}
        for item in ranked_items:
            cluster_id = (
                normalize_text(item.get("event_identity_key"))
                or normalize_text(item.get("cluster_id"))
                or normalize_text(item.get("url") or item.get("id"))
            )
            if not cluster_id:
                continue
            clusters.setdefault(cluster_id, []).append(item)

        for cluster_items in clusters.values():
            if not cluster_items:
                continue
            representatives = _choose_representatives(cluster_items)
            if not representatives:
                continue
            event_key, signature_tokens = _event_signature(topic, cluster_items)
            source_counts: dict[str, int] = {}
            first_seen_at = snapshot_at
            last_seen_at = snapshot_at
            confirmation_count = 0
            authority_score = 0.0
            authority_keys: set[str] = set()
            for item in cluster_items:
                source = normalize_text(item.get("source")).lower() or "unknown"
                source_counts[source] = source_counts.get(source, 0) + 1
                created_ts = parse_created_at_to_ts(item.get("created_at"))
                fetched_at = item.get("fetched_at")
                seen_at = int(created_ts or (fetched_at if isinstance(fetched_at, (int, float)) else snapshot_at))
                first_seen_at = min(first_seen_at, seen_at)
                last_seen_at = max(last_seen_at, seen_at)
                authority_score = max(
                    authority_score,
                    float(item.get("event_authority_score", item.get("source_authority_score", 0.0)) or 0.0),
                )
                if bool(item.get("source_confirmed")):
                    confirmation_count += 1
                    authority_key = normalize_text(
                        item.get("source_authority_key")
                        or item.get("external_domain")
                        or item.get("source_domain")
                        or source
                    ).lower()
                    if authority_key:
                        authority_keys.add(authority_key)
            entries.append(
                {
                    "captured_at": snapshot_at,
                    "generated_at": generated_at,
                    "digest_date": digest_date,
                    "topic": topic,
                    "event_key": event_key,
                    "signature_tokens": signature_tokens,
                    "snapshot_hits": 1,
                    "evidence_count": len(cluster_items),
                    "source_counts": source_counts,
                    "first_seen_at": first_seen_at,
                    "last_seen_at": last_seen_at,
                    "confirmation_count": confirmation_count,
                    "authority_score": round(authority_score, 2),
                    "authority_keys": sorted(authority_keys),
                    "representatives": representatives,
                }
            )
    return entries


def _pick_better_representative(current: dict[str, object] | None, candidate: dict[str, object]) -> dict[str, object]:
    if not isinstance(current, dict):
        return dict(candidate)
    current_score = (
        float(current.get("selection_score", 0.0) or 0.0),
        float(current.get("topic_quality_score", 0.0) or 0.0),
        float(current.get("score", 0.0) or 0.0),
        int(current.get("fetched_at", 0) or 0),
    )
    candidate_score = (
        float(candidate.get("selection_score", 0.0) or 0.0),
        float(candidate.get("topic_quality_score", 0.0) or 0.0),
        float(candidate.get("score", 0.0) or 0.0),
        int(candidate.get("fetched_at", 0) or 0),
    )
    return dict(candidate if candidate_score >= current_score else current)


def _normalize_signature_tokens(tokens: object) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    if not isinstance(tokens, (list, tuple, set)):
        return ordered
    for token in tokens:
        normalized = normalize_text(token).lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _should_merge_event_entry(
    *,
    topic: str,
    event_key: str,
    signature_tokens: list[str],
    current: dict[str, object],
) -> bool:
    if normalize_text(current.get("topic")) != topic:
        return False
    current_event_key = normalize_text(current.get("event_key"))
    if current_event_key == event_key:
        return True

    current_tokens = _normalize_signature_tokens(current.get("signature_tokens"))
    if not signature_tokens or not current_tokens:
        return False
    if signature_tokens[0] != current_tokens[0]:
        return False

    signature_token_set = set(signature_tokens)
    current_token_set = set(current_tokens)
    shared_count = len(signature_token_set & current_token_set)
    similarity = jaccard_similarity(signature_token_set, current_token_set)
    if shared_count >= 3 and shared_count == min(len(signature_token_set), len(current_token_set)):
        return True
    return (
        shared_count >= 4 and similarity >= 0.8
    )


def merge_event_pool_entries(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    merged: list[dict[str, object]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        topic = normalize_text(entry.get("topic"))
        event_key = normalize_text(entry.get("event_key"))
        if not topic or not event_key:
            continue
        signature_tokens = _normalize_signature_tokens(entry.get("signature_tokens"))
        current = next(
            (
                item
                for item in merged
                if _should_merge_event_entry(
                    topic=topic,
                    event_key=event_key,
                    signature_tokens=signature_tokens,
                    current=item,
                )
            ),
            None,
        )
        if current is None:
            merged.append(
                {
                "topic": topic,
                "event_key": event_key,
                "signature_tokens": list(signature_tokens),
                "snapshot_hits": max(int(entry.get("snapshot_hits") or 1), 1),
                "evidence_count": max(int(entry.get("evidence_count") or 0), 0),
                "source_counts": dict(entry.get("source_counts") or {}),
                "first_seen_at": max(int(entry.get("first_seen_at") or entry.get("captured_at") or 0), 0),
                "last_seen_at": max(int(entry.get("last_seen_at") or entry.get("captured_at") or 0), 0),
                "confirmation_count": max(int(entry.get("confirmation_count") or 0), 0),
                "authority_score": max(float(entry.get("authority_score") or 0.0), 0.0),
                "authority_keys": [str(key).strip().lower() for key in (entry.get("authority_keys") or []) if str(key).strip()],
                "representatives": {source: dict(item) for source, item in (entry.get("representatives") or {}).items() if isinstance(item, dict)},
                }
            )
            continue

        current["snapshot_hits"] = int(current.get("snapshot_hits", 1) or 1) + max(int(entry.get("snapshot_hits") or 1), 1)
        current["evidence_count"] = int(current.get("evidence_count", 0) or 0) + max(int(entry.get("evidence_count") or 0), 0)
        current["first_seen_at"] = min(int(current.get("first_seen_at") or 0), max(int(entry.get("first_seen_at") or entry.get("captured_at") or 0), 0))
        current["last_seen_at"] = max(int(current.get("last_seen_at") or 0), max(int(entry.get("last_seen_at") or entry.get("captured_at") or 0), 0))
        current["confirmation_count"] = int(current.get("confirmation_count", 0) or 0) + max(int(entry.get("confirmation_count") or 0), 0)
        current["authority_score"] = max(
            float(current.get("authority_score") or 0.0),
            float(entry.get("authority_score") or 0.0),
        )
        current_tokens = _normalize_signature_tokens(current.get("signature_tokens"))
        current["signature_tokens"] = _normalize_signature_tokens([*current_tokens, *signature_tokens])
        current_authority_keys = {
            normalize_text(key).lower()
            for key in (current.get("authority_keys") or [])
            if normalize_text(key)
        }
        current_authority_keys.update(
            normalize_text(key).lower()
            for key in (entry.get("authority_keys") or [])
            if normalize_text(key)
        )
        current["authority_keys"] = sorted(current_authority_keys)

        source_counts = current.get("source_counts") if isinstance(current.get("source_counts"), dict) else {}
        incoming_counts = entry.get("source_counts") if isinstance(entry.get("source_counts"), dict) else {}
        for source, value in incoming_counts.items():
            source_name = normalize_text(source).lower()
            if not source_name:
                continue
            source_counts[source_name] = int(source_counts.get(source_name, 0) or 0) + max(int(value or 0), 0)
        current["source_counts"] = source_counts

        representatives = current.get("representatives") if isinstance(current.get("representatives"), dict) else {}
        incoming_reps = entry.get("representatives") if isinstance(entry.get("representatives"), dict) else {}
        for source, item in incoming_reps.items():
            source_name = normalize_text(source).lower()
            if not source_name or not isinstance(item, dict):
                continue
            representatives[source_name] = _pick_better_representative(
                representatives.get(source_name) if isinstance(representatives.get(source_name), dict) else None,
                item,
            )
        current["representatives"] = representatives
    return merged


def build_topic_items_from_event_pool(entries: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    topics = {"crypto": [], "world": [], "hot": [], "custom": []}
    for entry in merge_event_pool_entries(entries):
        topic = normalize_text(entry.get("topic"))
        if topic not in topics:
            continue
        representatives = entry.get("representatives") if isinstance(entry.get("representatives"), dict) else {}
        hydrated_representatives: dict[str, dict[str, object]] = {}
        derived_authority_score = 0.0
        derived_authority_keys: set[str] = set()
        derived_confirmation_count = 0
        for source, representative in representatives.items():
            if not isinstance(representative, dict):
                continue
            hydrated = _hydrate_representative(topic, representative)
            hydrated_representatives[source] = hydrated
            derived_authority_score = max(
                derived_authority_score,
                float(hydrated.get("event_authority_score", hydrated.get("source_authority_score", 0.0)) or 0.0),
            )
            if bool(hydrated.get("source_confirmed")):
                derived_confirmation_count += 1
                authority_key = normalize_text(
                    hydrated.get("source_authority_key")
                    or hydrated.get("external_domain")
                    or hydrated.get("source_domain")
                    or source
                ).lower()
                if authority_key:
                    derived_authority_keys.add(authority_key)
        source_counts = entry.get("source_counts") if isinstance(entry.get("source_counts"), dict) else {}
        source_count = sum(1 for _, value in source_counts.items() if int(value or 0) > 0)
        first_seen_at = max(int(entry.get("first_seen_at") or 0), 0)
        last_seen_at = max(int(entry.get("last_seen_at") or 0), first_seen_at)
        window_span_hours = round(max(last_seen_at - first_seen_at, 0) / 3600.0, 2)
        authority_keys = {
            normalize_text(key).lower()
            for key in (entry.get("authority_keys") or [])
            if normalize_text(key)
        }
        authority_keys.update(derived_authority_keys)
        authority_count = len(authority_keys)
        confirmation_count = max(int(entry.get("confirmation_count") or 0), authority_count, derived_confirmation_count)
        authority_score = max(float(entry.get("authority_score") or 0.0), derived_authority_score)
        for source, representative in hydrated_representatives.items():
            item = dict(representative)
            item["event_key"] = normalize_text(entry.get("event_key"))
            item["window_hits"] = max(int(entry.get("snapshot_hits") or 1), 1)
            item["window_first_seen_at"] = first_seen_at
            item["window_last_seen_at"] = last_seen_at
            item["window_span_hours"] = window_span_hours
            item["event_evidence_count"] = max(int(entry.get("evidence_count") or 0), 0)
            item["event_source_count"] = source_count
            item["event_has_x"] = bool(int(source_counts.get("x", 0) or 0) > 0)
            item["event_source_counts"] = dict(source_counts)
            item["event_confirmation_count"] = confirmation_count
            item["event_authority_score"] = authority_score
            item["event_authority_count"] = authority_count
            item["source"] = source
            topics[topic].append(item)
    return topics
