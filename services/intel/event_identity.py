from __future__ import annotations

import hashlib
import re
from collections import Counter

from services.intel.text import normalize_text, normalize_tweet_text
from services.intel.topics import EVENT_STOPWORDS

EVENT_GENERIC_NOISE: set[str] = {
    "breaking", "urgent", "latest", "report", "reports", "official", "statement", "new", "major",
    "says", "said", "just", "update", "updates", "war", "crypto", "bitcoin", "world",
    "today", "yesterday", "tomorrow", "tonight", "usd", "market", "markets",
}

EVENT_CONTEXT_NOISE: set[str] = {
    "according", "after", "again", "amid", "analyst", "analysts", "chatter", "confirm", "confirmed", "confirms",
    "confirmation", "continue", "continued", "continues", "continuing", "drive", "driven", "drives", "filing",
    "first", "hit", "hits", "its", "largest", "live", "media", "minute", "minutes", "more",
    "most", "now", "odds", "official", "officials", "progress", "progresses", "progressing", "push",
    "pushes", "pushing", "response", "responses", "rise", "rises", "said", "say", "says", "second",
    "stage", "still", "their", "them", "they", "third", "watch", "watching", "yet",
}

EVENT_ENTITY_ALIASES: dict[str, tuple[str, ...]] = {
    "bitcoin": ("bitcoin", "btc", "比特币"),
    "ethereum": ("ethereum", "eth", "以太坊"),
    "stablecoin": ("stablecoin", "stablecoins", "usdt", "usdc", "tether", "稳定币"),
    "blackrock": ("blackrock",),
    "microstrategy": ("microstrategy", "strategy"),
    "binance": ("binance", "币安"),
    "coinbase": ("coinbase",),
    "solana": ("solana", "sol",),
    "sec": ("sec", "证券交易委员会"),
    "iran": ("iran", "iranian", "伊朗"),
    "israel": ("israel", "israeli", "以色列"),
    "gaza": ("gaza", "加沙"),
    "ukraine": ("ukraine", "ukrainian", "乌克兰"),
    "russia": ("russia", "russian", "俄罗斯"),
    "china": ("china", "chinese", "中国"),
    "taiwan": ("taiwan", "台湾"),
    "us": ("u.s.", "us", "united states", "america", "american", "美国"),
    "nato": ("nato", "北约"),
    "un": ("un", "united nations", "联合国"),
    "fed": ("fed", "federal reserve", "美联储"),
    "openai": ("openai",),
    "nvidia": ("nvidia", "英伟达"),
    "meta": ("meta",),
    "google": ("google",),
    "anthropic": ("anthropic",),
}

EVENT_ACTION_ALIASES: dict[str, tuple[str, ...]] = {
    "approval": ("approve", "approved", "approval", "通过", "审批"),
    "listing": ("listing", "listed", "delist", "delisted", "上市", "下架"),
    "buying": ("buy", "bought", "purchase", "purchased", "acquire", "acquired", "增持", "买入", "购入"),
    "inflow": ("inflow", "inflows", "outflow", "outflows"),
    "hack": ("hack", "hacked", "exploit", "exploited", "breach", "drain", "drained", "黑客", "漏洞", "被盗"),
    "liquidation": ("liquidation", "liquidated", "bankrupt", "bankruptcy", "depeg", "depegged", "爆仓", "破产", "脱锚"),
    "attack": ("attack", "attacked", "airstrike", "airstrikes", "strike", "strikes", "missile", "missiles", "drone", "bombing", "袭击", "空袭", "导弹"),
    "ceasefire": ("ceasefire", "truce", "停火"),
    "talks": ("talks", "negotiation", "negotiations", "summit", "deal", "agreement", "会谈", "谈判", "峰会", "协议"),
    "election": ("election", "vote", "voted", "elected", "coup", "选举", "投票", "政变"),
    "sanctions": ("sanction", "sanctions", "tariff", "ban", "banned", "restriction", "embargo", "制裁", "关税", "禁令"),
    "disaster": ("earthquake", "flood", "wildfire", "hurricane", "storm", "typhoon", "tsunami", "地震", "洪水", "野火", "飓风", "台风", "海啸"),
    "launch": ("launch", "release", "unveil", "发布"),
    "lawsuit": ("lawsuit", "sue", "investigation", "起诉", "调查"),
    "outage": ("outage", "blackout", "down", "offline", "disrupted", "停电", "故障", "中断"),
}


def _build_alias_index(alias_map: dict[str, tuple[str, ...]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for canonical, variants in alias_map.items():
        canonical_name = normalize_text(canonical).lower()
        if not canonical_name:
            continue
        index[canonical_name] = canonical_name
        for variant in variants:
            normalized = normalize_text(variant).lower()
            if normalized:
                index[normalized] = canonical_name
    return index


EVENT_ENTITY_ALIAS_INDEX = _build_alias_index(EVENT_ENTITY_ALIASES)
EVENT_ACTION_ALIAS_INDEX = _build_alias_index(EVENT_ACTION_ALIASES)


def _stable_unique(tokens: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        normalized = normalize_text(token).lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _normalize_identity_token(token: object, alias_index: dict[str, str]) -> str:
    normalized = normalize_text(token).lower()
    if not normalized:
        return ""
    return alias_index.get(normalized, normalized)


def _extract_identity_text_tokens(text: object) -> list[str]:
    lowered = normalize_tweet_text(text).lower()
    lowered = re.sub(r"https?://\S+", " ", lowered)
    tokens = re.findall(r"[a-z0-9_]{3,}", lowered)
    for chunk in re.findall(r"[\u4e00-\u9fff]{2,4}", lowered):
        tokens.append(chunk)
    return [token for token in tokens if token not in EVENT_STOPWORDS and token not in EVENT_GENERIC_NOISE]


def _collect_identity_terms(values: object, alias_index: dict[str, str], *, limit: int) -> list[str]:
    counter: Counter[str] = Counter()
    if isinstance(values, (list, tuple, set)):
        iterator = values
    else:
        iterator = []
    for value in iterator:
        normalized = _normalize_identity_token(value, alias_index)
        if normalized:
            counter[normalized] += 1
    return [token for token, _ in counter.most_common(limit)]


def _detect_terms_from_text(tokens: list[str], alias_index: dict[str, str], *, limit: int) -> list[str]:
    counter: Counter[str] = Counter()
    for token in tokens:
        canonical = alias_index.get(token)
        if canonical:
            counter[canonical] += 1
    return [token for token, _ in counter.most_common(limit)]


def _event_type_terms(event_type: str) -> list[str]:
    normalized_event_type = normalize_text(event_type).lower().replace("_", " ")
    tokens = re.findall(r"[a-z0-9]{3,}|[\u4e00-\u9fff]{2,4}", normalized_event_type)
    canonical_terms: list[str] = []
    for token in tokens:
        if not token:
            continue
        canonical_terms.append(
            EVENT_ENTITY_ALIAS_INDEX.get(token)
            or EVENT_ACTION_ALIAS_INDEX.get(token)
            or token
        )
    return [
        token
        for token in _stable_unique(canonical_terms)
        if token not in EVENT_STOPWORDS and token not in EVENT_GENERIC_NOISE
    ]


def _is_context_noise_token(token: str, *, exclude: set[str]) -> bool:
    if not token or token in exclude:
        return True
    if token in EVENT_STOPWORDS or token in EVENT_GENERIC_NOISE or token in EVENT_CONTEXT_NOISE:
        return True
    if token in EVENT_ENTITY_ALIAS_INDEX or token in EVENT_ACTION_ALIAS_INDEX:
        return True
    if re.fullmatch(r"\d+", token):
        return True
    return False


def _source_noise_terms(item: dict[str, object]) -> set[str]:
    noise_terms: set[str] = set()
    raw_values = [
        item.get("source_domain"),
        item.get("external_domain"),
        item.get("author"),
    ]
    for value in raw_values:
        normalized = normalize_text(value).lower()
        if not normalized:
            continue
        for token in re.findall(r"[a-z0-9]{3,}|[\u4e00-\u9fff]{2,4}", normalized.replace(".", " ")):
            if token in {"http", "https", "www", "com", "net", "org", "twitter", "reddit"}:
                continue
            noise_terms.add(token)
    return noise_terms


def _context_terms(tokens: list[str], *, exclude: set[str], limit: int) -> list[str]:
    counter: Counter[str] = Counter()
    for token in tokens:
        if _is_context_noise_token(token, exclude=exclude):
            continue
        counter[token] += 1
    return [token for token, _ in counter.most_common(limit)]


def build_item_event_identity(item: dict[str, object]) -> dict[str, object]:
    topic = normalize_text(item.get("topic")).lower()
    event_type = normalize_text(item.get("topic_event_type")).lower()
    event_type_terms = _event_type_terms(event_type)
    text_tokens = _extract_identity_text_tokens(item.get("text"))
    source_noise_terms = _source_noise_terms(item)

    entity_terms = _collect_identity_terms(item.get("topic_event_entity_hits"), EVENT_ENTITY_ALIAS_INDEX, limit=3)
    if not entity_terms:
        entity_terms = _detect_terms_from_text(text_tokens, EVENT_ENTITY_ALIAS_INDEX, limit=3)

    action_terms = _collect_identity_terms(item.get("topic_event_action_hits"), EVENT_ACTION_ALIAS_INDEX, limit=2)
    if not action_terms:
        action_terms = _detect_terms_from_text(text_tokens, EVENT_ACTION_ALIAS_INDEX, limit=2)

    evidence_terms = _collect_identity_terms(item.get("topic_event_evidence_hits"), EVENT_ACTION_ALIAS_INDEX, limit=2)
    exclude = {topic, event_type, *event_type_terms, *entity_terms, *action_terms, *evidence_terms, *source_noise_terms}
    context_terms = _context_terms(text_tokens, exclude=exclude, limit=2)

    identity_terms = _stable_unique([term for term in [event_type, *entity_terms, *action_terms, *context_terms] if term])
    raw_key = "|".join([topic, *identity_terms]).strip("|")
    identity_key = hashlib.sha1(raw_key.encode("utf-8", errors="ignore")).hexdigest()[:16] if raw_key else ""
    return {
        "event_identity_terms": identity_terms,
        "event_identity_key": identity_key,
        "event_identity_raw": raw_key,
    }


def build_group_event_identity(topic: str, items: list[dict[str, object]]) -> tuple[str, list[str]]:
    event_type_counter: Counter[str] = Counter()
    entity_counter: Counter[str] = Counter()
    action_counter: Counter[str] = Counter()
    context_counter: Counter[str] = Counter()

    for item in items:
        identity = build_item_event_identity(item)
        event_type = normalize_text(item.get("topic_event_type")).lower()
        if event_type:
            event_type_counter[event_type] += 1
        for token in identity.get("event_identity_terms", []):
            normalized = normalize_text(token).lower()
            if not normalized:
                continue
            if event_type and normalized == event_type:
                continue
            if normalized in EVENT_ACTION_ALIAS_INDEX:
                action_counter[normalized] += 1
            elif normalized in EVENT_ENTITY_ALIAS_INDEX.values():
                entity_counter[normalized] += 1
            else:
                context_counter[normalized] += 1

    top_event_type = ""
    if event_type_counter:
        top_event_type = sorted(event_type_counter.items(), key=lambda item: (-item[1], item[0]))[0][0]

    entity_terms = [token for token, _ in sorted(entity_counter.items(), key=lambda item: (-item[1], item[0]))[:3]]
    action_terms = [token for token, _ in sorted(action_counter.items(), key=lambda item: (-item[1], item[0]))[:2]]
    context_terms = [token for token, _ in sorted(context_counter.items(), key=lambda item: (-item[1], item[0]))[:2]]

    signature_terms = _stable_unique([term for term in [top_event_type, *entity_terms, *action_terms, *context_terms] if term])
    if not signature_terms:
        lead = items[0] if items else {}
        fallback = normalize_text(lead.get("url") or lead.get("id") or normalize_tweet_text(lead.get("text"))[:80])
        signature_terms = [fallback] if fallback else []

    raw_key = "|".join([normalize_text(topic).lower(), *signature_terms]).strip("|")
    event_key = hashlib.sha1(raw_key.encode("utf-8", errors="ignore")).hexdigest()[:20] if raw_key else ""
    return event_key, signature_terms
