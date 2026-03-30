from __future__ import annotations

import json
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from services.intel.text import extract_url_domain, normalize_text, strip_html_tags

PUBLIC_SOURCE_CACHE_TTL_SECONDS = 180
PUBLIC_SOURCE_USER_AGENT = "intel-monitor/20260326"

_PUBLIC_SOURCE_CACHE: dict[str, tuple[float, object]] = {}
_PUBLIC_SOURCE_CACHE_LOCK = threading.RLock()


def _load_cached(cache_key: str, ttl_seconds: int) -> object | None:
    now_value = time.time()
    with _PUBLIC_SOURCE_CACHE_LOCK:
        cached = _PUBLIC_SOURCE_CACHE.get(cache_key)
    if not cached:
        return None
    cached_at, payload = cached
    if now_value - cached_at > ttl_seconds:
        return None
    return payload


def _store_cached(cache_key: str, payload: object) -> object:
    with _PUBLIC_SOURCE_CACHE_LOCK:
        _PUBLIC_SOURCE_CACHE[cache_key] = (time.time(), payload)
        if len(_PUBLIC_SOURCE_CACHE) > 256:
            oldest_key = min(_PUBLIC_SOURCE_CACHE.items(), key=lambda item: item[1][0])[0]
            _PUBLIC_SOURCE_CACHE.pop(oldest_key, None)
    return payload


def _http_get_text(url: str, timeout: int = 20) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": PUBLIC_SOURCE_USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def _cached_json_request(url: str, timeout: int = 20) -> object:
    cache_key = f"json::{url}"
    cached = _load_cached(cache_key, PUBLIC_SOURCE_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    payload = json.loads(_http_get_text(url, timeout=timeout))
    return _store_cached(cache_key, payload)


def _cached_text_request(url: str, timeout: int = 20) -> str:
    cache_key = f"text::{url}"
    cached = _load_cached(cache_key, PUBLIC_SOURCE_CACHE_TTL_SECONDS)
    if isinstance(cached, str):
        return cached
    text = _http_get_text(url, timeout=timeout)
    return _store_cached(cache_key, text)


def _format_unix_ts(value: object) -> str:
    try:
        ts = float(value or 0)
    except Exception:
        ts = 0.0
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_reddit_search(query: str, limit: int) -> list[dict[str, object]]:
    safe_limit = min(max(int(limit or 0), 1), 25)
    encoded = urllib.parse.urlencode(
        {
            "q": normalize_text(query),
            "sort": "top",
            "t": "day",
            "limit": safe_limit,
            "raw_json": 1,
            "type": "link",
        }
    )
    url = f"https://www.reddit.com/search.json?{encoded}"
    payload = _cached_json_request(url, timeout=20)
    data = payload.get("data") if isinstance(payload, dict) else {}
    children = data.get("children") if isinstance(data, dict) else []
    rows: list[dict[str, object]] = []

    for child in children if isinstance(children, list) else []:
        node = child.get("data") if isinstance(child, dict) else {}
        if not isinstance(node, dict):
            continue
        permalink = normalize_text(node.get("permalink"))
        discussion_url = f"https://www.reddit.com{permalink}" if permalink else ""
        external_url = normalize_text(node.get("url_overridden_by_dest") or node.get("url"))
        title = strip_html_tags(node.get("title"))
        body = strip_html_tags(node.get("selftext") or node.get("selftext_html"))
        rows.append(
            {
                "id": normalize_text(node.get("id")),
                "title": title,
                "text": body,
                "author": normalize_text(node.get("author")),
                "subreddit": normalize_text(node.get("subreddit")),
                "url": discussion_url or external_url,
                "external_url": external_url if external_url and external_url != discussion_url else "",
                "created_at": _format_unix_ts(node.get("created_utc")),
                "score": node.get("score") or node.get("ups") or 0,
                "domain": extract_url_domain(external_url or discussion_url),
            }
        )
    return rows


def _find_first_text(element: ET.Element | None, *candidates: str) -> str:
    if element is None:
        return ""
    for name in candidates:
        child = element.find(name)
        if child is not None and child.text:
            return normalize_text(child.text)
    return ""


def fetch_rss_feed(feed_name: str, url: str, limit: int) -> list[dict[str, object]]:
    safe_limit = min(max(int(limit or 0), 1), 12)
    xml_text = _cached_text_request(url, timeout=20)
    root = ET.fromstring(xml_text)
    rows: list[dict[str, object]] = []

    for item in root.findall(".//item")[:safe_limit]:
        title = strip_html_tags(_find_first_text(item, "title"))
        description = strip_html_tags(
            _find_first_text(
                item,
                "description",
                "{http://purl.org/rss/1.0/modules/content/}encoded",
                "{http://www.w3.org/2005/Atom}summary",
            )
        )
        link = normalize_text(_find_first_text(item, "link", "{http://www.w3.org/2005/Atom}link"))
        guid = normalize_text(_find_first_text(item, "guid"))
        pub_date = normalize_text(
            _find_first_text(
                item,
                "pubDate",
                "{http://purl.org/dc/elements/1.1/}date",
                "{http://www.w3.org/2005/Atom}updated",
            )
        )
        rows.append(
            {
                "id": guid or link or title,
                "title": title,
                "text": description,
                "author": feed_name,
                "url": link,
                "created_at": pub_date,
                "domain": extract_url_domain(link),
                "feed_name": feed_name,
            }
        )
    return rows
