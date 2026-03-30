from __future__ import annotations

from dataclasses import dataclass, field

from services.intel.text import keyword_hits, normalize_text


@dataclass(frozen=True, slots=True)
class RssFeed:
    name: str
    url: str


@dataclass(frozen=True, slots=True)
class EventProfile:
    code: str
    label: str
    entity_keywords: frozenset[str]
    action_keywords: frozenset[str] = field(default_factory=frozenset)
    evidence_keywords: frozenset[str] = field(default_factory=frozenset)
    negative_keywords: frozenset[str] = field(default_factory=frozenset)
    min_score: float = 5.0
    require_entity: bool = True


@dataclass(frozen=True, slots=True)
class TopicRule:
    x_queries: tuple[str, ...]
    reddit_queries: tuple[str, ...]
    keywords: frozenset[str]
    allowed_subreddits: frozenset[str]
    rss_feeds: tuple[RssFeed, ...] = field(default_factory=tuple)
    news_keywords: frozenset[str] = field(default_factory=frozenset)
    promo_keywords: frozenset[str] = field(default_factory=frozenset)
    event_profiles: tuple[EventProfile, ...] = field(default_factory=tuple)
    x_limit_min: int = 12
    x_limit_max: int = 24
    reddit_limit_min: int = 12
    reddit_limit_max: int = 18


TRUSTED_SOURCE_DOMAIN_TIERS: dict[str, int] = {
    "apnews.com": 3,
    "bbc.co.uk": 3,
    "bbc.com": 3,
    "bloomberg.com": 3,
    "coindesk.com": 3,
    "ft.com": 3,
    "npr.org": 3,
    "nytimes.com": 3,
    "reuters.com": 3,
    "theblock.co": 3,
    "theguardian.com": 3,
    "wsj.com": 3,
    "cointelegraph.com": 2,
    "decrypt.co": 2,
    "dw.com": 2,
    "skynews.com": 2,
}


LOW_TRUST_EXTERNAL_DOMAINS: frozenset[str] = frozenset(
    {
        "reddit.com",
        "i.redd.it",
        "v.redd.it",
        "youtube.com",
        "youtu.be",
        "x.com",
        "twitter.com",
    }
)


TOPIC_RULES: dict[str, TopicRule] = {
    "crypto": TopicRule(
        x_queries=(
            "bitcoin OR btc OR ethereum OR eth OR crypto OR binance OR solana OR stablecoin OR defi",
            "altcoin OR memecoin OR layer2 OR web3",
        ),
        reddit_queries=(
            "bitcoin OR crypto OR ethereum OR binance OR solana",
        ),
        keywords=frozenset(
            {
                "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency", "binance",
                "solana", "stablecoin", "defi", "altcoin", "memecoin", "token", "airdrop",
                "layer2", "web3", "etf", "wallet", "exchange", "staking", "onchain",
                "on-chain", "blockchain", "黑客", "比特币", "以太坊", "加密", "币圈",
                "稳定币", "代币", "交易所", "链上", "质押",
            }
        ),
        allowed_subreddits=frozenset(
            {
                "bitcoin", "btc", "cryptocurrency", "ethereum", "ethtrader", "ethfinance",
                "binance", "solana", "defi", "cryptomarkets", "cryptotechnology",
                "cryptonews", "bitcoinmarkets",
            }
        ),
        rss_feeds=(
            RssFeed(name="CoinDesk", url="https://www.coindesk.com/arc/outboundfeeds/rss/"),
            RssFeed(name="The Block", url="https://www.theblock.co/rss.xml"),
            RssFeed(name="Decrypt", url="https://decrypt.co/feed"),
        ),
        news_keywords=frozenset(
            {
                "etf", "sec", "hack", "exploit", "stablecoin", "bankruptcy", "liquidation",
                "blackrock", "treasury", "fed", "whale", "listing", "delist", "approval",
                "approve", "approved", "purchase", "bought", "acquire", "acquired",
                "accumulate", "accumulating", "inflows", "outflows", "监管", "黑客", "爆仓",
                "上市", "下架", "购入", "增持", "审批", "通过",
            }
        ),
        promo_keywords=frozenset(
            {
                "giveaway", "airdrop", "join us", "live now", "roundtable", "spaces",
                "mint", "wl", "retweet", "follow", "gmgn", "contract address", "wallet:",
                "ca:", "ca：", "pump", "presale", "launchpad", "buy now", "100x",
                "altseason", "lfg", "bag", "accumulate", "public beta", "now live",
                "register to claim", "prize pool", "earn rewards", "soft staking",
                "claims go live",
            }
        ),
        event_profiles=(
            EventProfile(
                code="regulation_etf",
                label="监管/ETF",
                entity_keywords=frozenset(
                    {
                        "bitcoin", "btc", "ethereum", "eth", "etf", "sec", "blackrock",
                        "fidelity", "grayscale", "stablecoin", "regulation", "监管", "审批",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "approve", "approved", "approval", "denied", "delay", "filing",
                        "filed", "launch", "listing", "delist", "reserve", "bill", "law",
                        "rule", "regulated", "监管", "通过", "下架", "上市",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "application", "spot etf", "inflows", "outflows", "treasury",
                        "strategic reserve", "legislation", "policy", "audit", "13f",
                        "持仓", "储备",
                    }
                ),
                min_score=5.4,
            ),
            EventProfile(
                code="security_incident",
                label="安全事故",
                entity_keywords=frozenset(
                    {
                        "exchange", "protocol", "bridge", "wallet", "stablecoin", "bitcoin",
                        "btc", "ethereum", "eth", "solana", "binance", "coinbase", "bybit",
                        "usdt", "usdc", "交易所", "钱包", "协议",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "hack", "hacked", "exploit", "exploited", "breach", "drain",
                        "drained", "stolen", "attacker", "vulnerability", "frozen", "freeze",
                        "黑客", "攻击", "漏洞", "被盗", "冻结",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "loss", "losses", "funds", "wallet", "address", "recovery",
                        "blacklisted", "withdrawals", "reserves", "资金", "地址",
                    }
                ),
                min_score=5.0,
            ),
            EventProfile(
                code="market_stress",
                label="市场风险",
                entity_keywords=frozenset(
                    {
                        "stablecoin", "usdt", "usdc", "tether", "exchange", "binance",
                        "coinbase", "market", "token", "derivatives", "稳定币", "交易所",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "depeg", "depegged", "liquidation", "liquidated", "bankrupt",
                        "bankruptcy", "insolvent", "withdrawals", "halt", "halted", "suspend",
                        "suspended", "default", "爆仓", "破产", "清算", "脱锚", "暂停",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "peg", "reserve", "leverage", "margin", "exposure", "outflows",
                        "lawsuit", "defaults", "储备", "杠杆",
                    }
                ),
                min_score=5.2,
            ),
            EventProfile(
                code="institutional_flow",
                label="机构资金",
                entity_keywords=frozenset(
                    {
                        "bitcoin", "btc", "ethereum", "eth", "etf", "blackrock", "strategy",
                        "microstrategy", "treasury", "whale", "比特币", "以太坊", "鲸鱼",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "buy", "bought", "acquire", "acquired", "accumulate", "accumulated",
                        "holding", "holdings", "inflow", "inflows", "outflow", "outflows",
                        "purchase", "purchased", "增持", "买入", "购入", "减持",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "million", "billions", "treasury", "reserve", "wallet", "13f",
                        "onchain", "on-chain", "持仓", "链上",
                    }
                ),
                min_score=5.0,
            ),
        ),
    ),
    "world": TopicRule(
        x_queries=(
            "iran OR israel OR gaza OR ukraine OR russia OR china OR taiwan OR ceasefire OR missile OR election",
            "geopolitics OR sanctions OR airstrike OR military OR invasion OR diplomacy OR summit OR tariff",
        ),
        reddit_queries=(
            "iran OR israel OR gaza OR ukraine OR russia OR china OR taiwan OR election OR sanctions OR ceasefire",
            "geopolitics OR war OR military OR diplomacy OR tariff OR summit",
        ),
        keywords=frozenset(
            {
                "war", "ceasefire", "invasion", "attack", "airstrike", "strike", "missile",
                "sanction", "sanctions", "election", "earthquake", "flood", "wildfire",
                "diplomacy", "military", "troops", "conflict", "geopolitics", "tariff",
                "summit", "hostage", "coup", "white house", "parliament", "president",
                "prime minister", "ukraine", "russia", "iran", "israel", "gaza", "china",
                "taiwan", "syria", "iraq", "yemen", "north korea", "south korea", "nato",
                "联合国", "战争", "停火", "冲突", "制裁", "袭击", "导弹", "军事", "选举",
                "总统", "总理", "外交", "峰会", "地震", "洪水", "乌克兰", "俄罗斯", "伊朗",
                "以色列", "加沙", "中国", "台湾", "朝鲜", "韩国",
            }
        ),
        allowed_subreddits=frozenset(
            {
                "worldnews", "news", "geopolitics", "politics", "anime_titties",
                "europe", "middleeastnews", "ukraine", "china", "taiwan",
            }
        ),
        rss_feeds=(
            RssFeed(name="BBC World", url="https://feeds.bbci.co.uk/news/world/rss.xml"),
            RssFeed(name="NPR World", url="https://feeds.npr.org/1004/rss.xml"),
            RssFeed(name="Guardian World", url="https://www.theguardian.com/world/rss"),
            RssFeed(name="Sky World", url="https://feeds.skynews.com/feeds/rss/world.xml"),
            RssFeed(name="NYT World", url="https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
            RssFeed(name="DW", url="https://rss.dw.com/xml/rss-en-all"),
        ),
        news_keywords=frozenset(
            {
                "ceasefire", "invasion", "attack", "airstrike", "missile", "sanction",
                "sanctions", "election", "earthquake", "flood", "wildfire", "hostage",
                "coup", "troops", "war", "冲突", "停火", "战争", "制裁", "袭击", "导弹",
                "选举", "地震", "洪水",
            }
        ),
        event_profiles=(
            EventProfile(
                code="war_escalation",
                label="战争升级",
                entity_keywords=frozenset(
                    {
                        "ukraine", "russia", "iran", "israel", "gaza", "china", "taiwan",
                        "syria", "iraq", "yemen", "lebanon", "nato", "hostage",
                        "乌克兰", "俄罗斯", "伊朗", "以色列", "加沙", "中国", "台湾",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "attack", "attacked", "airstrike", "airstrikes", "missile",
                        "missiles", "drone", "troops", "invasion", "offensive", "strike",
                        "shelling", "bombing", "raid", "袭击", "空袭", "导弹", "入侵",
                        "轰炸", "军事",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "casualties", "killed", "wounded", "intercepted", "deployed",
                        "operation", "launch", "military", "伤亡", "拦截", "部署",
                    }
                ),
                min_score=5.2,
            ),
            EventProfile(
                code="ceasefire_diplomacy",
                label="停火/外交",
                entity_keywords=frozenset(
                    {
                        "ukraine", "russia", "iran", "israel", "gaza", "china", "taiwan",
                        "white house", "un", "united nations", "nato", "summit", "联合国",
                        "白宫", "峰会",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "ceasefire", "truce", "talks", "negotiation", "negotiations",
                        "summit", "deal", "agreement", "diplomacy", "mediated", "envoy",
                        "停火", "谈判", "会谈", "峰会", "外交", "协议",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "delegation", "minister", "statement", "proposal", "sanctions",
                        "aid corridor", "mediator", "代表团", "提案", "制裁",
                    }
                ),
                min_score=5.0,
            ),
            EventProfile(
                code="election_power",
                label="选举/权力变动",
                entity_keywords=frozenset(
                    {
                        "election", "president", "prime minister", "parliament", "senate",
                        "congress", "government", "cabinet", "opposition", "coup",
                        "white house", "议会", "总统", "总理", "政府", "选举", "政变",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "vote", "voted", "elected", "resign", "resigned", "impeachment",
                        "impeached", "coup", "cabinet", "coalition", "poll", "ballot",
                        "投票", "胜选", "辞职", "弹劾", "组阁",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "majority", "results", "runoff", "parliament", "cabinet", "sworn",
                        "多数", "结果",
                    }
                ),
                min_score=5.0,
            ),
            EventProfile(
                code="sanctions_trade",
                label="制裁/贸易",
                entity_keywords=frozenset(
                    {
                        "sanction", "sanctions", "tariff", "export", "import", "china",
                        "us", "eu", "russia", "iran", "制裁", "关税", "出口", "进口",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "ban", "banned", "restrict", "restricted", "restriction",
                        "blacklist", "tariff", "sanction", "embargo", "export control",
                        "禁令", "限制", "列入黑名单", "贸易战",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "duties", "measures", "trade", "shipment", "embargo", "措施",
                        "税率", "贸易",
                    }
                ),
                min_score=5.0,
            ),
            EventProfile(
                code="major_disaster",
                label="重大灾害",
                entity_keywords=frozenset(
                    {
                        "earthquake", "flood", "wildfire", "hurricane", "storm", "typhoon",
                        "tsunami", "地震", "洪水", "野火", "飓风", "台风", "海啸",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "killed", "injured", "evacuate", "evacuation", "emergency",
                        "rescue", "damage", "magnitude", "aftershock", "landfall",
                        "死亡", "伤者", "疏散", "救援", "预警",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "casualties", "deaths", "homes", "aid", "warning", "aftershock",
                        "伤亡", "房屋", "灾情",
                    }
                ),
                min_score=4.8,
            ),
        ),
    ),
    "hot": TopicRule(
        x_queries=(),
        reddit_queries=(
            "ai OR openai OR nvidia OR chip OR tariff OR outage OR antitrust OR earthquake OR wildfire OR merger",
        ),
        keywords=frozenset(
            {
                "ai", "artificial intelligence", "openai", "anthropic", "nvidia", "chip",
                "chips", "semiconductor", "tariff", "federal reserve", "fed", "inflation",
                "recession", "antitrust", "lawsuit", "supreme court", "merger", "acquisition",
                "ipo", "outage", "blackout", "cyberattack", "data breach", "earthquake",
                "flood", "wildfire", "hurricane", "storm", "pandemic", "vaccine", "nasa",
                "spacex", "rocket", "satellite", "climate", "tesla", "微软", "英伟达", "人工智能",
                "芯片", "关税", "停电", "故障", "地震", "洪水", "野火", "飓风", "疫情", "火箭",
            }
        ),
        allowed_subreddits=frozenset(
            {
                "news", "worldnews", "technology", "science", "futurology",
                "business", "economics", "hardware", "singularity",
            }
        ),
        rss_feeds=(
            RssFeed(name="BBC Top", url="https://feeds.bbci.co.uk/news/rss.xml"),
            RssFeed(name="NPR News", url="https://feeds.npr.org/1001/rss.xml"),
            RssFeed(name="BBC Tech", url="https://feeds.bbci.co.uk/news/technology/rss.xml"),
            RssFeed(name="NPR Tech", url="https://feeds.npr.org/1019/rss.xml"),
        ),
        news_keywords=frozenset(
            {
                "openai", "nvidia", "chip", "semiconductor", "tariff", "federal reserve",
                "rate cut", "rate hike", "antitrust", "lawsuit", "supreme court", "merger",
                "acquisition", "outage", "blackout", "cyberattack", "data breach",
                "earthquake", "flood", "wildfire", "hurricane", "pandemic", "vaccine",
                "rocket", "launch", "satellite", "ai", "人工智能", "芯片", "关税", "停电",
                "故障", "地震", "洪水", "野火", "飓风", "疫情", "火箭",
            }
        ),
        event_profiles=(
            EventProfile(
                code="ai_chip",
                label="AI/芯片",
                entity_keywords=frozenset(
                    {
                        "ai", "openai", "anthropic", "nvidia", "chip", "chips",
                        "semiconductor", "gpu", "microsoft", "google", "人工智能", "英伟达", "芯片",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "launch", "release", "unveil", "earnings", "funding", "partnership",
                        "ban", "lawsuit", "antitrust", "export", "发布", "起诉", "禁令",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "model", "api", "training", "h100", "b200", "cloud", "gpu",
                        "模型", "算力",
                    }
                ),
                min_score=5.0,
            ),
            EventProfile(
                code="outage_cyber",
                label="故障/安全",
                entity_keywords=frozenset(
                    {
                        "outage", "blackout", "cyberattack", "data breach", "breach",
                        "service", "platform", "停电", "故障", "网络攻击", "数据泄露",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "down", "offline", "restored", "disrupted", "hacked", "exposed",
                        "incident", "恢复", "中断", "瘫痪", "泄露",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "systems", "users", "millions", "services", "系统", "用户",
                    }
                ),
                min_score=4.8,
            ),
            EventProfile(
                code="business_policy",
                label="政策/商业",
                entity_keywords=frozenset(
                    {
                        "tariff", "federal reserve", "fed", "inflation", "recession",
                        "antitrust", "merger", "acquisition", "ipo", "关税", "并购", "通胀",
                    }
                ),
                action_keywords=frozenset(
                    {
                        "raise", "cut", "approve", "block", "sue", "investigation",
                        "acquire", "acquired", "merge", "merged", "加息", "降息", "批准",
                        "起诉", "调查",
                    }
                ),
                evidence_keywords=frozenset(
                    {
                        "deal", "court", "rates", "earnings", "guidance", "法院", "协议",
                    }
                ),
                min_score=4.8,
            ),
        ),
        x_limit_min=0,
        x_limit_max=0,
        reddit_limit_min=8,
        reddit_limit_max=12,
    ),
}


SIGNAL_BURST_KEYWORDS: dict[str, float] = {
    "breaking": 1.2,
    "urgent": 1.0,
    "ceasefire": 1.6,
    "invasion": 1.8,
    "strike": 1.2,
    "attack": 1.2,
    "sanction": 1.3,
    "election": 1.0,
    "earthquake": 1.4,
    "flood": 1.2,
    "war": 1.8,
    "冲突": 1.6,
    "停火": 1.6,
    "战争": 1.8,
    "制裁": 1.3,
    "地震": 1.4,
    "突发": 1.2,
    "etf": 1.4,
    "hack": 1.8,
    "exploit": 1.8,
    "liquidation": 1.6,
    "bankruptcy": 1.8,
    "regulation": 1.3,
    "sec": 1.1,
    "bitcoin": 0.8,
    "ethereum": 0.8,
    "stablecoin": 1.2,
    "btc": 0.8,
    "eth": 0.8,
    "黑客": 1.8,
    "爆仓": 1.6,
    "监管": 1.3,
}


EVENT_STOPWORDS: set[str] = {
    "the", "and", "for", "with", "that", "this", "from", "are", "was", "were", "will", "have", "has", "had",
    "you", "your", "our", "their", "about", "into", "over", "under", "between", "after", "before", "today", "just",
    "bitcoin", "crypto", "news", "world", "breaking", "thread", "update", "rt",
}


def _domain_tier(domain: object) -> int:
    normalized = normalize_text(domain).lower()
    if not normalized:
        return 0
    best_tier = 0
    for candidate, tier in TRUSTED_SOURCE_DOMAIN_TIERS.items():
        if normalized == candidate or normalized.endswith(f".{candidate}"):
            best_tier = max(best_tier, int(tier))
    return best_tier


def classify_source_reliability(
    source: object,
    *,
    source_domain: object = "",
    external_domain: object = "",
    subreddit: object = "",
    author: object = "",
) -> dict[str, object]:
    source_name = normalize_text(source).lower()
    source_domain_name = normalize_text(source_domain).lower()
    external_domain_name = normalize_text(external_domain).lower()
    subreddit_name = normalize_text(subreddit).lower().removeprefix("r/")
    author_name = normalize_text(author).lower().lstrip("@")

    best_domain = external_domain_name or source_domain_name
    best_tier = _domain_tier(best_domain)
    source_role = "unknown"
    trust_tier = 0
    authority_score = 0.0
    discovery_weight = 0.0
    confirmation_weight = 0.0
    confirmed = False
    authority_key = ""

    if source_name == "x":
        source_role = "discovery"
        trust_tier = 1
        discovery_weight = 2.6
        authority_score = 0.7
        authority_key = f"x:{author_name}" if author_name else "x"
    elif source_name == "rss":
        source_role = "confirmation"
        trust_tier = max(best_tier, 2 if source_domain_name else 1)
        authority_score = 1.0 + (trust_tier * 0.6)
        discovery_weight = 1.2
        confirmation_weight = 1.2 + (trust_tier * 0.45)
        confirmed = True
        authority_key = best_domain or source_domain_name or "rss"
    elif source_name == "reddit":
        if best_domain and best_domain not in LOW_TRUST_EXTERNAL_DOMAINS and best_tier >= 2:
            source_role = "confirmation"
            trust_tier = best_tier
            authority_score = 0.8 + (trust_tier * 0.65)
            discovery_weight = 0.7
            confirmation_weight = 1.0 + (trust_tier * 0.45)
            confirmed = True
            authority_key = best_domain
        else:
            source_role = "community"
            trust_tier = 1 if subreddit_name else 0
            authority_score = 0.3 + (0.3 if subreddit_name else 0.0)
            discovery_weight = 0.5 + (0.2 if subreddit_name else 0.0)
            authority_key = f"reddit:{subreddit_name}" if subreddit_name else "reddit"

    return {
        "source_role": source_role,
        "source_trust_tier": trust_tier,
        "source_authority_score": round(authority_score, 2),
        "source_discovery_weight": round(discovery_weight, 2),
        "source_confirmation_weight": round(confirmation_weight, 2),
        "source_confirmed": confirmed,
        "source_authority_key": authority_key,
    }


def classify_topic_event(rule: TopicRule | None, searchable_text: object) -> dict[str, object]:
    default = {
        "topic_event_type": "",
        "topic_event_label": "",
        "topic_event_score": 0.0,
        "topic_event_matched": False,
        "topic_event_entity_hits": [],
        "topic_event_action_hits": [],
        "topic_event_evidence_hits": [],
        "topic_event_negative_hits": [],
    }
    if rule is None or not rule.event_profiles:
        return default

    searchable = normalize_text(searchable_text).lower()
    if not searchable:
        return default

    best: dict[str, object] = dict(default)
    best_score = 0.0

    for profile in rule.event_profiles:
        entity_hits = keyword_hits(searchable, set(profile.entity_keywords))
        action_hits = keyword_hits(searchable, set(profile.action_keywords))
        evidence_hits = keyword_hits(searchable, set(profile.evidence_keywords))
        negative_hits = keyword_hits(searchable, set(profile.negative_keywords))

        positive_score = (len(entity_hits) * 2.8) + (len(action_hits) * 2.2) + (len(evidence_hits) * 1.3)
        score = positive_score - (len(negative_hits) * 2.6)
        qualifies = bool(
            score >= profile.min_score
            and (entity_hits or not profile.require_entity)
            and (action_hits or evidence_hits)
        )
        if not qualifies or score < best_score:
            continue

        best_score = score
        best = {
            "topic_event_type": profile.code,
            "topic_event_label": profile.label,
            "topic_event_score": round(score, 2),
            "topic_event_matched": True,
            "topic_event_entity_hits": sorted(entity_hits),
            "topic_event_action_hits": sorted(action_hits),
            "topic_event_evidence_hits": sorted(evidence_hits),
            "topic_event_negative_hits": sorted(negative_hits),
        }

    return best


def get_topic_rule(topic: str) -> TopicRule | None:
    return TOPIC_RULES.get(topic)


def build_default_fixed_topics() -> dict[str, dict[str, list[str]]]:
    return {
        topic: {
            "x_queries": list(rule.x_queries),
            "reddit_queries": list(rule.reddit_queries),
        }
        for topic, rule in TOPIC_RULES.items()
    }
