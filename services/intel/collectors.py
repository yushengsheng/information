from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from services.intel.opencli import fetch_opencli_json, run_opencli_json_command
from services.intel.public_sources import fetch_reddit_search, fetch_rss_feed
from services.intel.text import (
    extract_url_domain,
    is_low_signal_text,
    keyword_hit_count,
    keyword_hits,
    looks_url_only,
    normalize_searchable_text,
    normalize_text,
    normalize_tweet_text,
)
from services.intel.topics import classify_source_reliability, classify_topic_event, get_topic_rule

CRYPTO_HYPE_PATTERNS = (
    r"(?<![a-z0-9])buy now(?![a-z0-9])",
    r"(?<![a-z0-9])lfg(?![a-z0-9])",
    r"(?<![a-z0-9])100x(?![a-z0-9])",
    r"(?<![a-z0-9])altseason(?![a-z0-9])",
    r"(?<![a-z0-9])bag(?:ged)?(?![a-z0-9])",
    r"(?<![a-z0-9])smart ones(?![a-z0-9])",
    r"(?<![a-z0-9])chase later(?![a-z0-9])",
)
X_EMPTY_RETRY_DELAY_SECONDS = 2.0


def prefer_fetch_limit(limit_hint: int, *, minimum: int, maximum: int) -> int:
    return min(max(limit_hint * 2, minimum), maximum)


def _collect_x_records_for_topic(
    topic: str,
    jobs: list[tuple[list[str], int]],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for command, timeout in jobs:
        try:
            rows = fetch_opencli_json(command, timeout)
        except Exception:
            rows = []
        for row in rows:
            record = build_topic_record("x", topic, command, row)
            if record:
                items.append(record)
    return items


def _collect_custom_x_records(
    jobs: list[tuple[str, list[str]]],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for username, command in jobs:
        try:
            rows = fetch_opencli_json(command, 90)
        except Exception:
            rows = []
        for row in rows:
            record = {
                "source": "x",
                "topic": "custom",
                "id": normalize_text(row.get("id")),
                "author": normalize_text(row.get("author") or username),
                "text": normalize_tweet_text(row.get("text")),
                "url": normalize_text(row.get("url")),
                "created_at": normalize_text(row.get("created_at")),
                "score": row.get("likes") or 0,
                "owner": username,
                "fetched_at": int(time.time()),
            }
            if record["text"] and not looks_url_only(str(record["text"])):
                items.append(record)
    return items


def is_promotional_text(text: str, promo_keywords: frozenset[str]) -> bool:
    searchable = normalize_searchable_text(text)
    if not searchable:
        return False
    hashtag_hits = len(re.findall(r"#[a-z0-9_]{2,}", searchable))
    cashtag_hits = len(re.findall(r"\$[a-z][a-z0-9_]{1,9}", searchable))
    if keyword_hit_count(searchable, set(promo_keywords)) > 0:
        return True
    if re.search(r"\b0x[a-f0-9]{8,}\b", searchable):
        return True
    if searchable.count("@") >= 3:
        return True
    if hashtag_hits >= 4 or cashtag_hits >= 2:
        return True
    if any(re.search(pattern, searchable) for pattern in CRYPTO_HYPE_PATTERNS):
        return True
    return False


def annotate_topic_relevance(item: dict[str, object], topic: str) -> bool:
    rule = get_topic_rule(topic)
    if rule is None:
        item["topic_match_count"] = 0
        item["topic_quality_score"] = 0.0
        item["topic_low_signal"] = False
        return True

    text = normalize_tweet_text(item.get("text"))
    source = normalize_text(item.get("source")).lower()
    subreddit = normalize_text(item.get("subreddit")).lower().removeprefix("r/")
    searchable = normalize_searchable_text(text, item.get("url"), subreddit)
    matched_keywords = keyword_hits(searchable, set(rule.keywords))
    match_count = len(matched_keywords)
    subreddit_match = bool(subreddit and subreddit in rule.allowed_subreddits)
    low_signal = is_low_signal_text(text)
    matched_news_keywords = keyword_hits(searchable, set(rule.news_keywords))
    news_match_count = len(matched_news_keywords)
    promotional = is_promotional_text(text, rule.promo_keywords) if rule.promo_keywords else False
    source_domain = normalize_text(item.get("source_domain")).lower()
    external_domain = normalize_text(item.get("external_domain")).lower()
    source_meta = classify_source_reliability(
        source,
        source_domain=source_domain,
        external_domain=external_domain,
        subreddit=subreddit,
        author=item.get("author"),
    )
    item.update(source_meta)
    event_meta = classify_topic_event(rule, searchable)
    item.update(event_meta)
    source_confirmed = bool(item.get("source_confirmed"))
    source_authority_score = float(item.get("source_authority_score", 0.0) or 0.0)
    event_matched = bool(item.get("topic_event_matched"))
    event_score = float(item.get("topic_event_score", 0.0) or 0.0)
    reddit_linked_news = bool(
        external_domain
        and external_domain not in {"reddit.com", "i.redd.it", "v.redd.it", "youtube.com", "youtu.be", "x.com", "twitter.com"}
    )
    try:
        engagement = max(float(item.get("score", 0) or 0), 0.0)
    except Exception:
        engagement = 0.0
    x_breaking_hint = bool(re.search(r"(?<![a-z0-9])(?:breaking|urgent|just in)(?![a-z0-9])", searchable)) or "突发" in searchable
    topic_overlap_counts: dict[str, int] = {}
    if topic == "hot":
        for overlap_topic in ("crypto", "world"):
            overlap_rule = get_topic_rule(overlap_topic)
            if overlap_rule is None:
                continue
            overlap_hits = keyword_hits(searchable, set(overlap_rule.keywords) | set(overlap_rule.news_keywords))
            topic_overlap_counts[overlap_topic] = len(overlap_hits)

    relevant = subreddit_match or match_count > 0
    if source == "reddit":
        relevant = subreddit_match and not low_signal and (
            event_matched
            or (source_confirmed and news_match_count > 0 and match_count >= 2)
            or (reddit_linked_news and news_match_count > 0 and match_count >= 2)
            or (match_count >= 3 and news_match_count >= 1)
        )
    elif source == "rss":
        relevant = match_count > 0 or news_match_count > 0 or event_matched
        if topic in {"crypto", "world"}:
            relevant = event_matched or (source_confirmed and news_match_count > 0 and match_count >= 2)
    elif source == "x":
        relevant = (
            event_matched
            or news_match_count > 0
            or match_count >= 2
            or (match_count >= 1 and engagement >= 200 and not low_signal and not promotional)
            or (match_count >= 1 and x_breaking_hint and engagement >= 80)
        )
        if topic == "world" and not event_matched:
            relevant = relevant and (
                news_match_count >= 1
                or (match_count >= 3 and engagement >= 180 and not low_signal)
            )
        if topic == "crypto" and not event_matched:
            relevant = relevant and (
                news_match_count >= 1
                or (match_count >= 3 and engagement >= 220 and not promotional)
            )
    if low_signal and not subreddit_match and match_count == 0:
        relevant = False
    if source == "x" and low_signal and news_match_count == 0 and engagement < 300:
        relevant = False
    if topic == "hot" and max(topic_overlap_counts.values() or [0]) >= 2:
        relevant = False
    if promotional and news_match_count == 0:
        relevant = False
    if topic in {"crypto", "world"} and source != "x" and not event_matched and not source_confirmed:
        relevant = False

    quality_score = (
        match_count * 4.0
        + (2.0 if subreddit_match else 0.0)
        + (1.2 if not low_signal else 0.0)
        + min(len(text), 240) / 240.0
        + (event_score * 1.35)
        + (source_authority_score * 1.1)
        + (1.6 if source_confirmed else 0.0)
    )
    if rule.news_keywords:
        quality_score += news_match_count * 6.0
        if promotional:
            quality_score -= 5.0
    if source == "x":
        quality_score += min(engagement, 600.0) / 180.0
        if low_signal:
            quality_score -= 3.0
        if promotional:
            quality_score -= 6.0
        if event_matched:
            quality_score += 1.4

    item["topic_match_count"] = match_count + (1 if subreddit_match else 0)
    item["topic_quality_score"] = quality_score
    item["topic_low_signal"] = low_signal
    item["topic_news_match_count"] = news_match_count
    item["topic_promotional"] = promotional
    item["subreddit_allowed"] = subreddit_match
    if topic_overlap_counts:
        for overlap_topic, overlap_count in topic_overlap_counts.items():
            item[f"topic_overlap_{overlap_topic}"] = overlap_count
    return relevant


def collect_x_items(node: object) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []

    if isinstance(node, list) and all(isinstance(item, dict) for item in node):
        for row in node:
            assert isinstance(row, dict)
            text = normalize_tweet_text(row.get("full_text") or row.get("text") or row.get("content"))
            item = {
                "id": normalize_text(row.get("id") or row.get("tweet_id") or row.get("rest_id")),
                "text": text,
                "url": normalize_text(row.get("url") or row.get("permalink")),
                "created_at": normalize_text(row.get("created_at") or row.get("time") or row.get("date")),
                "author": normalize_text(row.get("author") or row.get("user") or row.get("username")),
                "likes": row.get("likes"),
                "retweets": row.get("retweets"),
            }
            if item["id"] or item["url"] or item["text"]:
                items.append(item)
    else:
        def walk(obj: object) -> None:
            if isinstance(obj, dict):
                text = normalize_tweet_text(obj.get("full_text") or obj.get("text") or obj.get("content"))
                if text:
                    items.append(
                        {
                            "id": normalize_text(obj.get("id") or obj.get("tweet_id") or obj.get("rest_id")),
                            "text": text,
                            "url": normalize_text(obj.get("url") or obj.get("permalink")),
                            "created_at": normalize_text(obj.get("created_at") or obj.get("time") or obj.get("date")),
                            "author": normalize_text(obj.get("author") or obj.get("user") or obj.get("username")),
                            "likes": obj.get("likes"),
                            "retweets": obj.get("retweets"),
                        }
                    )
                for value in obj.values():
                    walk(value)
            elif isinstance(obj, list):
                for child in obj:
                    walk(child)

        walk(node)

    deduped: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in items:
        key = f"{item.get('id', '')}::{item.get('text', '')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def enrich_items_with_thread(items: list[dict[str, object]], max_enrich: int = 3) -> list[dict[str, object]]:
    enriched = 0
    for item in items:
        if enriched >= max_enrich:
            break
        text = normalize_tweet_text(item.get("text"))
        if text and not looks_url_only(text):
            item["text"] = text
            continue

        tweet_ref = normalize_text(item.get("id") or item.get("url"))
        if not tweet_ref:
            continue

        try:
            parsed, _ = run_opencli_json_command(
                ["twitter", "thread", tweet_ref, "--limit", "1", "-f", "json"],
                timeout=45,
            )
            thread_items = collect_x_items(parsed)
            if not thread_items:
                continue
            best_text = normalize_tweet_text(thread_items[0].get("text"))
            if best_text:
                item["text"] = best_text
            if not item.get("created_at"):
                item["created_at"] = normalize_text(thread_items[0].get("created_at"))
            if not item.get("author"):
                item["author"] = normalize_text(thread_items[0].get("author"))
            enriched += 1
        except Exception:
            continue

    return items


def build_topic_record(source: str, topic: str, command: object, row: dict[str, object]) -> dict[str, object] | None:
    if source == "x":
        record = {
            "source": "x",
            "topic": topic,
            "id": normalize_text(row.get("id")),
            "author": normalize_text(row.get("author")),
            "text": normalize_tweet_text(row.get("text")),
            "url": normalize_text(row.get("url")),
            "created_at": normalize_text(row.get("created_at")),
            "score": row.get("likes") or row.get("views") or 0,
            "source_domain": extract_url_domain(row.get("url")),
            "fetched_at": int(time.time()),
        }
    elif source == "reddit":
        title = normalize_tweet_text(row.get("title"))
        body = normalize_tweet_text(row.get("text"))
        record = {
            "source": "reddit",
            "topic": topic,
            "id": normalize_text(row.get("id") or row.get("url")),
            "author": normalize_text(row.get("author")),
            "text": title if not body else f"{title}；{body}",
            "url": normalize_text(row.get("url")),
            "external_url": normalize_text(row.get("external_url")),
            "created_at": normalize_text(row.get("created_at")),
            "score": row.get("score") or row.get("upvotes") or 0,
            "subreddit": normalize_text(row.get("subreddit")),
            "source_domain": normalize_text(row.get("domain")) or extract_url_domain(row.get("url")),
            "external_domain": extract_url_domain(row.get("external_url")),
            "source_label": "Reddit",
            "fetched_at": int(time.time()),
        }
    else:
        title = normalize_tweet_text(row.get("title"))
        body = normalize_tweet_text(row.get("text"))
        feed_name = normalize_text(row.get("feed_name") or getattr(command, "name", "RSS"))
        record = {
            "source": "rss",
            "topic": topic,
            "id": normalize_text(row.get("id") or row.get("url") or title),
            "author": feed_name,
            "text": title if not body else f"{title}；{body}",
            "url": normalize_text(row.get("url")),
            "created_at": normalize_text(row.get("created_at")),
            "score": 0,
            "source_domain": normalize_text(row.get("domain")) or extract_url_domain(row.get("url")),
            "source_label": feed_name,
            "fetched_at": int(time.time()),
        }

    if not record["text"] or looks_url_only(str(record["text"])):
        return None
    if not annotate_topic_relevance(record, topic):
        return None
    return record


def collect_topic_items(cfg: dict[str, object], topic: str, limit_hint: int) -> list[dict[str, object]]:
    rule = get_topic_rule(topic)
    fixed = cfg.get("fixed") if isinstance(cfg.get("fixed"), dict) else {}
    topic_cfg = fixed.get(topic) if isinstance(fixed, dict) else {}
    if not isinstance(topic_cfg, dict):
        topic_cfg = {}

    x_queries = topic_cfg.get("x_queries", list(rule.x_queries) if rule else [])
    if not isinstance(x_queries, list):
        x_queries = list(rule.x_queries) if rule else []
    reddit_queries = topic_cfg.get("reddit_queries", list(rule.reddit_queries) if rule else [])
    if not isinstance(reddit_queries, list):
        reddit_queries = list(rule.reddit_queries) if rule else []

    x_limit = prefer_fetch_limit(
        limit_hint,
        minimum=rule.x_limit_min if rule else 12,
        maximum=rule.x_limit_max if rule else 24,
    )
    reddit_limit = prefer_fetch_limit(
        limit_hint,
        minimum=rule.reddit_limit_min if rule else 12,
        maximum=rule.reddit_limit_max if rule else 18,
    )
    rss_limit = prefer_fetch_limit(limit_hint, minimum=4, maximum=8)

    x_jobs: list[tuple[list[str], int]] = []
    public_jobs: list[tuple[str, object, int]] = []
    for query in x_queries[:4]:
        q = normalize_text(query)
        if q:
            x_jobs.append((["twitter", "search", q, "--limit", str(x_limit), "-f", "json"], 90))
    for query in reddit_queries[:4]:
        q = normalize_text(query)
        if q:
            public_jobs.append(("reddit", q, reddit_limit))
    for feed in rule.rss_feeds if rule else ():
        public_jobs.append(("rss", feed, rss_limit))

    items: list[dict[str, object]] = []
    # opencli 的 X 抓取会实际拉起浏览器。这里改为串行，避免一次批量开出多个 Chrome 窗口。
    items.extend(_collect_x_records_for_topic(topic, x_jobs))
    if x_jobs and not items:
        time.sleep(X_EMPTY_RETRY_DELAY_SECONDS)
        items.extend(_collect_x_records_for_topic(topic, x_jobs))

    worker_count = min(6, len(public_jobs)) if public_jobs else 0
    if worker_count == 0:
        return items

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {}
        for source, command, timeout in public_jobs:
            if source == "reddit":
                future = executor.submit(fetch_reddit_search, str(command), timeout)
            else:
                feed = command
                future = executor.submit(fetch_rss_feed, feed.name, feed.url, timeout)
            future_map[future] = (source, command)
        for future in as_completed(future_map):
            source, command = future_map[future]
            try:
                rows = future.result()
            except Exception:
                rows = []
            for row in rows:
                record = build_topic_record(source, topic, command, row)
                if record:
                    items.append(record)

    return items


def collect_custom_user_items(cfg: dict[str, object], limit_hint: int) -> list[dict[str, object]]:
    custom = cfg.get("custom") if isinstance(cfg.get("custom"), dict) else {}
    users = custom.get("x_users") if isinstance(custom, dict) else []
    if not isinstance(users, list):
        users = []

    fetch_limit = prefer_fetch_limit(limit_hint, minimum=6, maximum=12)
    jobs: list[tuple[str, list[str]]] = []
    for raw_user in users[:20]:
        username = str(raw_user or "").strip().lstrip("@")
        if not username:
            continue
        query = f"from:{username} -filter:replies"
        jobs.append((username, ["twitter", "search", query, "--limit", str(fetch_limit), "-f", "json"]))

    if not jobs:
        return []

    items = _collect_custom_x_records(jobs)
    if jobs and not items:
        time.sleep(X_EMPTY_RETRY_DELAY_SECONDS)
        items = _collect_custom_x_records(jobs)
    return items
