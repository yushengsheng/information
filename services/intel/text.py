from __future__ import annotations

import html
import re
import threading
import time
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

_TRANSLATION_CACHE: dict[str, str] = {}
_TRANSLATION_LOCK = threading.RLock()
TRANSLATION_WORKERS = 4


def parse_x_username(value: object) -> str:
    username = str(value or "").strip().lstrip("@")
    if not username:
        raise ValueError("请输入 X 用户名")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,32}", username):
        raise ValueError("X 用户名格式无效（仅支持字母/数字/下划线，最长 32 位）")
    return username


def parse_x_limit(value: object) -> int:
    if value in (None, ""):
        return 10
    limit = int(value)
    if limit <= 0 or limit > 50:
        raise ValueError("条数范围为 1-50")
    return limit


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_tweet_text(value: object) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    return html.unescape(text)


def normalize_searchable_text(*parts: object) -> str:
    text = " ".join(normalize_tweet_text(part) for part in parts if part is not None)
    lowered = text.lower()
    lowered = re.sub(r"https?://\S+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def strip_html_tags(value: object) -> str:
    text = normalize_tweet_text(value)
    if not text:
        return ""
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_url_domain(value: object) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    try:
        host = urlparse(raw).netloc.lower().strip()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def keyword_hits(text: str, keywords: set[str]) -> set[str]:
    searchable = normalize_text(text).lower()
    if not searchable:
        return set()

    hits: set[str] = set()
    for keyword in keywords:
        term = normalize_text(keyword).lower()
        if not term:
            continue
        if re.search(r"[\u4e00-\u9fff]", term):
            if term in searchable:
                hits.add(term)
            continue
        if re.fullmatch(r"[a-z0-9][a-z0-9_\- ]*[a-z0-9]", term):
            escaped = re.escape(term).replace(r"\ ", r"\s+")
            if " " not in term and "_" not in term and "-" not in term:
                escaped = f"{escaped}(?:s|es)?"
            if re.search(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", searchable):
                hits.add(term)
            continue
        if term in searchable:
            hits.add(term)
    return hits


def keyword_hit_count(text: str, keywords: set[str]) -> int:
    return len(keyword_hits(text, keywords))


def contains_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def should_translate_to_chinese(text: str) -> bool:
    cleaned = normalize_tweet_text(text)
    if not cleaned:
        return False
    if contains_chinese(cleaned):
        return False
    return bool(re.search(r"[A-Za-z]", cleaned))


def translate_text_to_chinese(text: str) -> str:
    source = normalize_tweet_text(text)
    if not source:
        return source
    if not should_translate_to_chinese(source):
        return source

    with _TRANSLATION_LOCK:
        cached = _TRANSLATION_CACHE.get(source)
    if cached:
        return cached

    try:
        from deep_translator import GoogleTranslator  # type: ignore

        translated = GoogleTranslator(source="auto", target="zh-CN").translate(source)
        final_text = normalize_tweet_text(translated) or source
    except Exception:
        final_text = source

    with _TRANSLATION_LOCK:
        _TRANSLATION_CACHE[source] = final_text
    return final_text


def translate_items_for_display(items: list[dict[str, object]]) -> list[dict[str, object]]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    pending: dict[str, list[dict[str, object]]] = {}
    for item in items:
        original_text = normalize_tweet_text(item.get("text"))
        item["original_text"] = original_text
        item["text"] = original_text
        if should_translate_to_chinese(original_text):
            pending.setdefault(original_text, []).append(item)

    if not pending:
        return items

    worker_count = min(TRANSLATION_WORKERS, len(pending))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(translate_text_to_chinese, source_text): source_text
            for source_text in pending
        }
        for future in as_completed(future_map):
            source_text = future_map[future]
            try:
                translated_text = future.result()
            except Exception:
                translated_text = source_text
            for item in pending[source_text]:
                item["text"] = translated_text
    return items


def looks_url_only(text: str) -> bool:
    cleaned = text.strip()
    if not cleaned:
        return True
    return bool(re.fullmatch(r"https?://\S+", cleaned))


def is_low_signal_text(text: str) -> bool:
    cleaned = normalize_tweet_text(text)
    if not cleaned:
        return True
    url_hits = re.findall(r"https?://\S+", cleaned)
    without_urls = re.sub(r"https?://\S+", " ", cleaned)
    without_mentions = re.sub(r"@[A-Za-z0-9_]{1,32}", " ", without_urls)
    without_spaces = re.sub(r"\s+", "", without_mentions)
    core = re.sub(r"[\-—_~`!@#$%^&*()+=\[\]{}|\\:;\"'，。！？、,.<>/?]+", "", without_spaces)
    if len(core) < 8:
        return True
    if url_hits and len(core) < 15:
        return True
    return False


def item_dedupe_key(item: dict[str, object]) -> str:
    url = normalize_text(item.get("url"))
    if url:
        return f"url::{url}"
    source = normalize_text(item.get("source"))
    item_id = normalize_text(item.get("id"))
    if source and item_id:
        return f"id::{source}::{item_id}"
    text = normalize_tweet_text(item.get("text"))
    normalized = re.sub(r"\s+", " ", text).strip().lower()
    return f"text::{normalized[:180]}"


def short_summary_text(text: str, max_len: int = 120) -> str:
    value = normalize_tweet_text(text)
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "…"


def parse_created_at_to_ts(value: object) -> float | None:
    raw = normalize_text(value)
    if not raw:
        return None
    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%a %b %d %H:%M:%S %z %Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
    ]
    for fmt in candidates:
        try:
            return time.mktime(time.strptime(raw, fmt))
        except Exception:
            continue
    try:
        return parsedate_to_datetime(raw).timestamp()
    except Exception:
        pass
    return None
