from __future__ import annotations

import re
import time

from services.intel.event_identity import build_item_event_identity
from services.intel.text import item_dedupe_key, keyword_hits, normalize_text, normalize_tweet_text, parse_created_at_to_ts
from services.intel.topics import EVENT_STOPWORDS, SIGNAL_BURST_KEYWORDS

SOURCE_WEIGHTS: dict[str, float] = {
    "x": 3.4,
    "rss": 2.5,
    "reddit": 1.7,
}

CLUSTER_SIMILARITY_THRESHOLD = 0.32


def extract_event_tokens(text: str) -> set[str]:
    lowered = normalize_tweet_text(text).lower()
    lowered = re.sub(r"https?://\S+", " ", lowered)
    tokens = set(re.findall(r"[a-z0-9_]{3,}", lowered))
    tokens = {token for token in tokens if token not in EVENT_STOPWORDS}

    for term in re.findall(r"[\u4e00-\u9fff]{2,4}", normalize_tweet_text(text)):
        tokens.add(term)
    return tokens


def jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    union = len(a | b)
    if union == 0:
        return 0.0
    return len(a & b) / union


def burst_keyword_score(text: str) -> float:
    lowered = normalize_tweet_text(text).lower()
    score = 0.0
    for keyword in keyword_hits(lowered, set(SIGNAL_BURST_KEYWORDS)):
        score += SIGNAL_BURST_KEYWORDS.get(keyword, 0.0)
    return score


def source_weight(item: dict[str, object]) -> float:
    source = normalize_text(item.get("source")).lower()
    weight = SOURCE_WEIGHTS.get(source, 1.0) * 0.55
    weight += max(float(item.get("source_discovery_weight", 0.0) or 0.0), 0.0)
    weight += max(float(item.get("source_authority_score", 0.0) or 0.0), 0.0) * 0.75
    weight += max(float(item.get("source_confirmation_weight", 0.0) or 0.0), 0.0) * 0.55
    if bool(item.get("topic_event_matched")):
        weight += 0.8
    if source == "reddit" and bool(item.get("subreddit_allowed")):
        weight += 0.2
    return weight


def _cluster_seed_score(item: dict[str, object]) -> float:
    return (
        float(item.get("topic_quality_score", 0.0))
        + source_weight(item)
        + (float(item.get("score", 0.0) or 0.0) ** 0.5)
    )


def _cluster_item_score(item: dict[str, object]) -> float:
    return (
        float(item.get("topic_quality_score", 0.0))
        + float(item.get("signal_burst", 0.0))
        + float(item.get("signal_novelty", 0.0))
        + source_weight(item)
    )


def _assign_event_clusters(items: list[dict[str, object]]) -> None:
    clusters: list[dict[str, object]] = []
    ordered = sorted(items, key=_cluster_seed_score, reverse=True)

    for item in ordered:
        tokens = item.get("event_tokens") if isinstance(item.get("event_tokens"), set) else set()
        identity_key = normalize_text(item.get("event_identity_key"))
        identity_terms = {
            normalize_text(term).lower()
            for term in (item.get("event_identity_terms") or [])
            if normalize_text(term)
        }
        event_type = normalize_text(item.get("topic_event_type")).lower()
        best_cluster: dict[str, object] | None = None
        best_similarity = 0.0

        for cluster in clusters:
            cluster_tokens = cluster.get("tokens") if isinstance(cluster.get("tokens"), set) else set()
            similarity = jaccard_similarity(tokens, cluster_tokens)
            cluster_identity_keys = cluster.get("identity_keys") if isinstance(cluster.get("identity_keys"), set) else set()
            cluster_identity_terms = cluster.get("identity_terms") if isinstance(cluster.get("identity_terms"), set) else set()
            cluster_event_types = cluster.get("event_types") if isinstance(cluster.get("event_types"), set) else set()
            same_identity = bool(identity_key and identity_key in cluster_identity_keys)
            structured_similarity = bool(
                event_type
                and event_type in cluster_event_types
                and len(identity_terms & cluster_identity_terms) >= 2
            )
            if same_identity or structured_similarity:
                best_similarity = max(best_similarity, 1.0 if same_identity else 0.72)
                best_cluster = cluster
                if same_identity:
                    break
            if similarity > best_similarity:
                best_similarity = similarity
                best_cluster = cluster

        shared_terms = len(tokens & (best_cluster.get("tokens") if isinstance(best_cluster, dict) and isinstance(best_cluster.get("tokens"), set) else set()))
        structured_shared_terms = len(
            identity_terms
            & (best_cluster.get("identity_terms") if isinstance(best_cluster, dict) and isinstance(best_cluster.get("identity_terms"), set) else set())
        )
        if best_cluster is not None and (
            best_similarity >= CLUSTER_SIMILARITY_THRESHOLD
            or shared_terms >= 3
            or structured_shared_terms >= 2
        ):
            cluster_items = best_cluster.get("items") if isinstance(best_cluster.get("items"), list) else []
            cluster_items.append(item)
            best_cluster["items"] = cluster_items
            best_cluster["tokens"] = (best_cluster.get("tokens") if isinstance(best_cluster.get("tokens"), set) else set()) | tokens
            best_cluster["identity_terms"] = (
                (best_cluster.get("identity_terms") if isinstance(best_cluster.get("identity_terms"), set) else set())
                | identity_terms
            )
            if identity_key:
                cluster_identity_keys = best_cluster.get("identity_keys") if isinstance(best_cluster.get("identity_keys"), set) else set()
                cluster_identity_keys.add(identity_key)
                best_cluster["identity_keys"] = cluster_identity_keys
            if event_type:
                cluster_event_types = best_cluster.get("event_types") if isinstance(best_cluster.get("event_types"), set) else set()
                cluster_event_types.add(event_type)
                best_cluster["event_types"] = cluster_event_types
        else:
            clusters.append(
                {
                    "id": f"cluster-{len(clusters) + 1}",
                    "tokens": set(tokens),
                    "identity_terms": set(identity_terms),
                    "identity_keys": {identity_key} if identity_key else set(),
                    "event_types": {event_type} if event_type else set(),
                    "items": [item],
                }
            )

    for cluster in clusters:
        cluster_items = cluster.get("items") if isinstance(cluster.get("items"), list) else []
        ranked_cluster_items = sorted(
            [item for item in cluster_items if isinstance(item, dict)],
            key=_cluster_item_score,
            reverse=True,
        )
        sources = {normalize_text(item.get("source")).lower() for item in ranked_cluster_items if normalize_text(item.get("source"))}
        cluster["items"] = ranked_cluster_items
        cluster["sources"] = sources

        for index, item in enumerate(ranked_cluster_items, 1):
            own_source = normalize_text(item.get("source")).lower()
            cross_sources = sorted(source for source in sources if source and source != own_source)
            item["cluster_id"] = cluster.get("id")
            item["cluster_rank"] = index
            item["cluster_size"] = len(ranked_cluster_items)
            item["cluster_source_count"] = len(sources)
            item["cluster_has_x"] = "x" in sources
            item["signal_cross_sources"] = cross_sources
            item["signal_cross_count"] = len(cross_sources)


def apply_event_signals(items: list[dict[str, object]]) -> list[dict[str, object]]:
    now_value = time.time()

    for item in items:
        text = normalize_tweet_text(item.get("text"))
        item["event_tokens"] = extract_event_tokens(text)
        item.update(build_item_event_identity(item))

        try:
            hot_score = max(float(item.get("score", 0) or 0), 0.0)
        except Exception:
            hot_score = 0.0

        created_ts = parse_created_at_to_ts(item.get("created_at"))
        if created_ts is None:
            fetched_at = item.get("fetched_at")
            created_ts = float(fetched_at) if isinstance(fetched_at, (int, float)) else now_value

        age_hours = max((now_value - created_ts) / 3600.0, 0.0)
        novelty = max(0.0, 1.0 - min(age_hours, 36.0) / 36.0)
        authority_score = max(
            float(item.get("event_authority_score", item.get("source_authority_score", 0.0)) or 0.0),
            0.0,
        )
        confirmation_count = max(
            int(item.get("event_confirmation_count", 1 if item.get("source_confirmed") else 0) or 0),
            0,
        )

        item["signal_hot"] = hot_score
        item["signal_burst"] = burst_keyword_score(text)
        item["signal_novelty"] = novelty
        item["signal_source_weight"] = source_weight(item)
        item["signal_authority"] = authority_score
        item["signal_confirmation"] = confirmation_count
        item["signal_cross_sources"] = []
        item["signal_cross_count"] = 0

    _assign_event_clusters(items)

    for item in items:
        hot = float(item.get("signal_hot", 0.0))
        burst = float(item.get("signal_burst", 0.0))
        novelty = float(item.get("signal_novelty", 0.0))
        source_score = float(item.get("signal_source_weight", 0.0))
        authority = float(item.get("signal_authority", 0.0))
        confirmation = float(item.get("signal_confirmation", 0.0))
        cross_count = float(item.get("signal_cross_count", 0.0))
        cluster_size = float(item.get("cluster_size", 1.0))
        cluster_source_count = float(item.get("cluster_source_count", 1.0))
        cluster_rank = float(item.get("cluster_rank", 1.0))
        window_hits = max(float(item.get("window_hits", 1.0) or 1.0), 1.0)
        window_span_hours = max(float(item.get("window_span_hours", 0.0) or 0.0), 0.0)
        event_match = 1.0 if bool(item.get("topic_event_matched")) else 0.0
        event_score = max(float(item.get("topic_event_score", 0.0) or 0.0), 0.0)

        cluster_diversity = min(cluster_size, 3.0) * 0.8 + min(cluster_source_count, 3.0) * 1.4
        lead_bonus = 1.4 if cluster_rank <= 1 else max(0.0, 1.1 - ((cluster_rank - 1.0) * 0.45))
        window_score = min(window_hits - 1.0, 6.0) * 1.8 + min(window_span_hours, 18.0) / 6.0
        item["signal_total"] = (
            (hot ** 0.5)
            + (burst * 2.2)
            + (novelty * 3.0)
            + (cross_count * 4.0)
            + (source_score * 2.0)
            + (authority * 2.4)
            + (confirmation * 3.4)
            + cluster_diversity
            + lead_bonus
            + window_score
            + (event_match * 4.2)
        )
        item["selection_score"] = (
            float(item.get("topic_quality_score", 0.0)) * 1.45
            + float(item.get("signal_total", 0.0))
            + (event_score * 1.15)
        )

    return items


def rank_items(items: list[dict[str, object]]) -> list[dict[str, object]]:
    ranked = sorted(
        apply_event_signals(items),
        key=lambda item: (
            float(item.get("selection_score", 0.0)),
            float(item.get("topic_quality_score", 0.0)),
            float(item.get("signal_confirmation", 0.0)),
            float(item.get("signal_authority", 0.0)),
            float(item.get("signal_cross_count", 0.0)),
            float(item.get("signal_hot", 0.0)),
        ),
        reverse=True,
    )
    for item in ranked:
        item.pop("event_tokens", None)
    return ranked


def dedupe_items(items: list[dict[str, object]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in items:
        key = item_dedupe_key(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def apply_sent_filter(items: list[dict[str, object]], sent_keys: dict[str, object]) -> list[dict[str, object]]:
    return [item for item in items if item_dedupe_key(item) not in sent_keys]


def _select_from_groups(groups: list[list[dict[str, object]]], limit: int) -> list[dict[str, object]]:
    if limit <= 0:
        return []

    selected: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    seen_clusters: set[str] = set()

    for group in groups:
        for item in group:
            item_key = item_dedupe_key(item)
            cluster_key = normalize_text(item.get("cluster_id")) or item_key
            if item_key in seen_keys or cluster_key in seen_clusters:
                continue
            selected.append(item)
            seen_keys.add(item_key)
            seen_clusters.add(cluster_key)
            if len(selected) >= limit:
                return selected[:limit]

    for group in groups:
        for item in group:
            item_key = item_dedupe_key(item)
            cluster_key = normalize_text(item.get("cluster_id")) or item_key
            if item_key in seen_keys:
                continue
            if cluster_key in seen_clusters:
                continue
            selected.append(item)
            seen_keys.add(item_key)
            seen_clusters.add(cluster_key)
            if len(selected) >= limit:
                return selected[:limit]

    return selected[:limit]


def _cluster_key(item: dict[str, object]) -> str:
    return normalize_text(item.get("cluster_id")) or item_dedupe_key(item)


def _is_x_priority_candidate(item: dict[str, object]) -> bool:
    if normalize_text(item.get("source")).lower() != "x":
        return False
    if bool(item.get("topic_low_signal")) or bool(item.get("topic_promotional")):
        return False

    topic = normalize_text(item.get("topic")).lower()
    event_matched = bool(item.get("topic_event_matched"))
    news_match_count = int(item.get("topic_news_match_count", 0) or 0)
    match_count = int(item.get("topic_match_count", 0) or 0)
    cross_count = int(item.get("signal_cross_count", 0) or 0)
    cluster_source_count = int(item.get("cluster_source_count", 0) or 0)
    confirmation_count = int(item.get("event_confirmation_count", 0) or 0)
    hot = float(item.get("signal_hot", 0.0) or 0.0)
    quality = float(item.get("topic_quality_score", 0.0) or 0.0)
    authority = float(item.get("event_authority_score", item.get("source_authority_score", 0.0)) or 0.0)
    strong_news = bool(news_match_count >= 2 or (news_match_count >= 1 and (match_count >= 2 or hot >= 250.0)))

    if topic == "crypto":
        corroborated = bool(
            (cross_count > 0 or cluster_source_count > 1 or confirmation_count >= 1)
            and quality >= 12.0
            and (event_matched or match_count >= 2 or news_match_count >= 1 or hot >= 200.0)
        )
        return bool(
            strong_news
            or (event_matched and quality >= 11.0 and (hot >= 50.0 or authority >= 1.4))
            or corroborated
            or (match_count >= 3 and quality >= 14.0 and hot >= 60.0)
        )
    if topic == "world":
        corroborated = bool(
            (cross_count > 0 or cluster_source_count > 1 or confirmation_count >= 1)
            and quality >= 12.0
            and (event_matched or news_match_count >= 1 or match_count >= 2 or hot >= 200.0)
        )
        return bool(
            strong_news
            or (event_matched and quality >= 11.5 and (authority >= 1.4 or hot >= 80.0))
            or corroborated
            or (news_match_count >= 1 and hot >= 120.0 and quality >= 14.0)
            or (match_count >= 3 and quality >= 14.0 and hot >= 150.0)
        )
    return bool(
        event_matched
        or
        news_match_count > 0
        or cross_count > 0
        or cluster_source_count > 1
        or match_count >= 3
        or (match_count >= 2 and (hot >= 80.0 or quality >= 12.0))
        or quality >= 18.0
    )


def _cluster_preferred_items(items: list[dict[str, object]], preferred_source: str) -> list[dict[str, object]]:
    grouped_items: dict[str, list[dict[str, object]]] = {}
    cluster_order: list[str] = []
    for item in items:
        cluster_key = _cluster_key(item)
        if cluster_key not in grouped_items:
            grouped_items[cluster_key] = []
            cluster_order.append(cluster_key)
        grouped_items[cluster_key].append(item)

    ordered: list[dict[str, object]] = []
    for cluster_key in cluster_order:
        cluster_items = grouped_items.get(cluster_key, [])
        preferred_candidate = next(
            (
                item
                for item in cluster_items
                if normalize_text(item.get("source")).lower() == preferred_source and _is_x_priority_candidate(item)
            ),
            None,
        )
        fallback_nonpreferred = next(
            (item for item in cluster_items if normalize_text(item.get("source")).lower() != preferred_source),
            None,
        )
        fallback_any = cluster_items[0] if cluster_items else None
        if preferred_candidate is not None:
            ordered.append(preferred_candidate)
        elif fallback_nonpreferred is not None:
            ordered.append(fallback_nonpreferred)
        elif fallback_any is not None:
            ordered.append(fallback_any)
    return ordered


def select_items_with_x_priority(items: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    preferred_reps = _cluster_preferred_items(items, "x")
    event_confirmed = [
        item
        for item in preferred_reps
        if bool(item.get("topic_event_matched")) and (
            int(item.get("event_confirmation_count", 0) or 0) >= 1 or bool(item.get("source_confirmed"))
        )
    ]
    event_x_preferred = [
        item
        for item in preferred_reps
        if (
            normalize_text(item.get("source")).lower() == "x"
            and bool(item.get("topic_event_matched"))
            and item not in event_confirmed
        )
    ]
    x_preferred_strong = [
        item
        for item in preferred_reps
        if (
            normalize_text(item.get("source")).lower() == "x"
            and item not in event_confirmed
            and item not in event_x_preferred
            and _is_x_priority_candidate(item)
        )
    ]
    x_preferred_weak = [
        item
        for item in preferred_reps
        if (
            normalize_text(item.get("source")).lower() == "x"
            and item not in event_confirmed
            and item not in event_x_preferred
            and item not in x_preferred_strong
        )
    ]
    other_preferred = [
        item
        for item in preferred_reps
        if (
            normalize_text(item.get("source")).lower() != "x"
            and item not in event_confirmed
            and item not in event_x_preferred
        )
    ]
    x_priority_cap = max(1, (limit * 3 + 4) // 5)
    x_preferred_primary = x_preferred_strong[:x_priority_cap]
    x_preferred_overflow = x_preferred_strong[x_priority_cap:]
    x_rest_strong = [
        item
        for item in items
        if (
            normalize_text(item.get("source")).lower() == "x"
            and item not in x_preferred_strong
            and item not in x_preferred_weak
            and _is_x_priority_candidate(item)
        )
    ]
    x_rest_weak = [
        item
        for item in items
        if (
            normalize_text(item.get("source")).lower() == "x"
            and item not in x_preferred_strong
            and item not in x_preferred_weak
            and item not in x_rest_strong
        )
    ]
    other_rest = [item for item in items if normalize_text(item.get("source")).lower() != "x" and item not in other_preferred]
    return _select_from_groups(
        [
            event_confirmed,
            event_x_preferred,
            x_preferred_primary,
            other_preferred,
            other_rest,
            x_preferred_overflow,
            x_rest_strong,
            x_preferred_weak,
            x_rest_weak,
        ],
        limit,
    )


def select_world_items(items: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    strongly_confirmed = [
        item
        for item in items
        if (
            int(item.get("event_confirmation_count", 0) or 0) >= 1
            or int(item.get("signal_cross_count", 0) or 0) >= 1
            or int(item.get("cluster_source_count", 0) or 0) >= 2
        )
    ]
    matched_confirmed = [item for item in strongly_confirmed if bool(item.get("topic_event_matched"))]
    other_confirmed = [item for item in strongly_confirmed if item not in matched_confirmed]
    matched_fallback = [item for item in items if item not in strongly_confirmed and bool(item.get("topic_event_matched"))]
    fallback = [item for item in items if item not in strongly_confirmed and item not in matched_fallback]
    return _select_from_groups(
        [
            select_items_with_x_priority(matched_confirmed, limit),
            select_items_with_x_priority(other_confirmed, limit),
            select_items_with_x_priority(matched_fallback, limit),
            select_items_with_x_priority(fallback, limit),
        ],
        limit,
    )


def _is_persistent_candidate(item: dict[str, object]) -> bool:
    if bool(item.get("topic_low_signal")) or bool(item.get("topic_promotional")):
        return False

    topic = normalize_text(item.get("topic")).lower()
    quality = float(item.get("topic_quality_score", 0.0) or 0.0)
    if quality < 10.0:
        return False

    window_hits = max(int(item.get("window_hits", 0) or 0), 0)
    window_span_hours = max(float(item.get("window_span_hours", 0.0) or 0.0), 0.0)
    event_source_count = max(int(item.get("event_source_count", 0) or 0), 0)
    confirmation_count = max(int(item.get("event_confirmation_count", 0) or 0), 0)
    cross_count = max(int(item.get("signal_cross_count", 0) or 0), 0)
    hot = max(float(item.get("signal_hot", 0.0) or 0.0), 0.0)
    match_count = max(int(item.get("topic_match_count", 0) or 0), 0)
    news_match_count = max(int(item.get("topic_news_match_count", 0) or 0), 0)
    event_matched = bool(item.get("topic_event_matched"))

    if window_hits < 2:
        return False
    if topic in {"crypto", "world"} and not event_matched and confirmation_count < 1:
        return False

    has_time_confirmation = window_span_hours >= 1.0
    has_source_confirmation = event_source_count >= 2 or cross_count >= 1 or confirmation_count >= 1
    has_strength_confirmation = event_matched or news_match_count >= 1 or match_count >= 2 or hot >= 120.0
    return bool(has_time_confirmation or has_source_confirmation or has_strength_confirmation)


def select_persistent_items(items: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    if limit <= 0:
        return []

    strong = [
        item
        for item in items
        if _is_persistent_candidate(item)
        and (
            max(int(item.get("event_source_count", 0) or 0), 0) >= 2
            or max(int(item.get("event_confirmation_count", 0) or 0), 0) >= 1
            or max(int(item.get("signal_cross_count", 0) or 0), 0) >= 1
            or max(float(item.get("window_span_hours", 0.0) or 0.0), 0.0) >= 3.0
        )
    ]
    qualified = [item for item in items if _is_persistent_candidate(item) and item not in strong]
    return _select_from_groups([strong, qualified], limit)


def select_hot_items(items: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    if limit <= 0:
        return []
    confirmed = [
        item
        for item in items
        if (
            int(item.get("event_confirmation_count", 0) or 0) >= 1
            or int(item.get("signal_cross_count", 0) or 0) >= 1
            or int(item.get("cluster_source_count", 0) or 0) >= 2
        )
    ]
    confirmed_event = [item for item in confirmed if bool(item.get("topic_event_matched"))]
    confirmed_other = [item for item in confirmed if item not in confirmed_event]
    rss_priority = [item for item in items if normalize_text(item.get("source")).lower() == "rss" and item not in confirmed]
    reddit_priority = [item for item in items if normalize_text(item.get("source")).lower() == "reddit" and item not in confirmed]
    x_priority = [
        item
        for item in items
        if normalize_text(item.get("source")).lower() == "x" and item not in confirmed and _is_x_priority_candidate(item)
    ]
    fallback = [item for item in items if item not in confirmed and item not in rss_priority and item not in reddit_priority and item not in x_priority]
    return _select_from_groups([confirmed_event, confirmed_other, rss_priority, reddit_priority, x_priority, fallback], limit)
