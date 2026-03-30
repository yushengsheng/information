from __future__ import annotations

import time
from collections import Counter
from datetime import datetime

from app_config import APP_VERSION
from services.intel.ai import build_summary_status
from services.intel.store import load_intel_config, load_latest_digest, load_observability_history
from services.intel.text import normalize_text

_SEVERITY_RANK = {"ok": 0, "info": 1, "warn": 2, "critical": 3}
_TREND_WINDOW_SECONDS = 24 * 60 * 60


def _now_iso(now_ts: int) -> str:
    return datetime.fromtimestamp(max(int(now_ts), 0)).astimezone().isoformat(timespec="seconds")


def _parse_iso_ts(value: object) -> int:
    raw = normalize_text(value)
    if not raw:
        return 0
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return 0
    if parsed.tzinfo is None:
        return 0
    try:
        return int(parsed.timestamp())
    except Exception:
        return 0


def _configured_x_topics(cfg: dict[str, object]) -> set[str]:
    topics: set[str] = set()
    fixed = cfg.get("fixed") if isinstance(cfg.get("fixed"), dict) else {}
    for topic in ("crypto", "world", "hot"):
        topic_cfg = fixed.get(topic) if isinstance(fixed, dict) else {}
        x_queries = topic_cfg.get("x_queries") if isinstance(topic_cfg, dict) else []
        if isinstance(x_queries, list) and any(normalize_text(query) for query in x_queries):
            topics.add(topic)

    custom = cfg.get("custom") if isinstance(cfg.get("custom"), dict) else {}
    x_users = custom.get("x_users") if isinstance(custom, dict) else []
    if isinstance(x_users, list) and any(normalize_text(user) for user in x_users):
        topics.add("custom")
    return topics


def _coerce_source_counts(build_stats: dict[str, object]) -> dict[str, dict[str, int]]:
    source_counts = build_stats.get("source_counts") if isinstance(build_stats.get("source_counts"), dict) else {}
    normalized: dict[str, dict[str, int]] = {}
    for topic, counts in source_counts.items():
        topic_name = normalize_text(topic).lower()
        if not topic_name or not isinstance(counts, dict):
            continue
        normalized[topic_name] = {}
        for source, value in counts.items():
            source_name = normalize_text(source).lower()
            if not source_name:
                continue
            try:
                normalized[topic_name][source_name] = max(int(value or 0), 0)
            except Exception:
                normalized[topic_name][source_name] = 0
    return normalized


def _sum_source_counts(source_counts: dict[str, dict[str, int]], topics: set[str] | None = None) -> dict[str, int]:
    selected_topics = topics if topics else set(source_counts.keys())
    totals = {"x": 0, "rss": 0, "reddit": 0}
    for topic in selected_topics:
        topic_counts = source_counts.get(topic, {})
        for source in totals:
            totals[source] += max(int(topic_counts.get(source, 0) or 0), 0)
    totals["total"] = sum(totals.values())
    return totals


def _select_active_payload(latest_digest: dict[str, object], task_snapshot: dict[str, object] | None) -> tuple[dict[str, object], str]:
    latest_payload = latest_digest if isinstance(latest_digest, dict) else {}
    task = task_snapshot.get("task") if isinstance(task_snapshot, dict) and isinstance(task_snapshot.get("task"), dict) else {}
    task_result = task.get("result") if isinstance(task.get("result"), dict) else {}
    if not task_result:
        return latest_payload, "latest_digest"

    latest_generated_ts = _parse_iso_ts(latest_payload.get("generated_at"))
    task_updated_ts = _parse_iso_ts(task.get("updated_at"))
    if task_updated_ts >= latest_generated_ts:
        return task_result, "build_task"
    return latest_payload, "latest_digest"


def _build_alert(severity: str, code: str, title: str, detail: str, hint: str = "") -> dict[str, str]:
    return {
        "severity": severity,
        "code": code,
        "title": title,
        "detail": detail,
        "hint": hint,
    }


def _summary_fallback_ratio(summary_stats: dict[str, object]) -> float:
    total_items = max(int(summary_stats.get("total_items") or 0), 0)
    if total_items <= 0:
        return 0.0
    fallback_items = max(int(summary_stats.get("fallback_items") or 0), 0)
    return fallback_items / total_items


def _coerce_history_entries(entries: object) -> list[dict[str, object]]:
    rows = entries if isinstance(entries, list) else []
    normalized: list[dict[str, object]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        try:
            captured_at = max(int(item.get("captured_at") or 0), 0)
        except Exception:
            captured_at = 0
        codes_raw = item.get("alert_codes") if isinstance(item.get("alert_codes"), list) else []
        alert_codes = [normalize_text(code).lower() for code in codes_raw if normalize_text(code)]
        normalized.append(
            {
                "captured_at": captured_at,
                "alert_codes": alert_codes,
                "level": normalize_text(item.get("level")).lower() or "ok",
                "source": normalize_text(item.get("source")).lower(),
            }
        )
    normalized.sort(key=lambda item: int(item.get("captured_at") or 0))
    return normalized


def _build_trend_summary(
    history_entries: list[dict[str, object]],
    alerts: list[dict[str, str]],
    *,
    now_ts: int,
) -> dict[str, object]:
    cutoff_ts = max(int(now_ts) - _TREND_WINDOW_SECONDS, 0)
    recent_entries = [entry for entry in history_entries if int(entry.get("captured_at") or 0) >= cutoff_ts]
    repeated_codes: Counter[str] = Counter()
    recent_issue_runs = 0
    for entry in recent_entries:
        codes = {
            normalize_text(code).lower()
            for code in (entry.get("alert_codes") if isinstance(entry.get("alert_codes"), list) else [])
            if normalize_text(code)
        }
        if codes:
            recent_issue_runs += 1
        for code in codes:
            repeated_codes[code] += 1

    current_titles = {
        normalize_text(alert.get("code")).lower(): normalize_text(alert.get("title")) or normalize_text(alert.get("code"))
        for alert in alerts
        if isinstance(alert, dict)
    }
    for alert in alerts:
        code = normalize_text(alert.get("code")).lower()
        if not code:
            continue
        recent_count = repeated_codes.get(code, 0)
        if recent_count > 0:
            alert["recent_count"] = str(recent_count)

    repeat_items = [
        {
            "code": code,
            "title": current_titles.get(code) or code,
            "count": count,
        }
        for code, count in repeated_codes.most_common()
        if count >= 2
    ][:3]
    if repeat_items:
        status_text = "近 24 小时重复异常：" + "；".join(f"{item['title']} {item['count']} 次" for item in repeat_items)
    elif recent_issue_runs:
        status_text = f"近 24 小时已采样 {len(recent_entries)} 次，其中 {recent_issue_runs} 次出现异常，但暂未形成重复告警。"
    elif recent_entries:
        status_text = f"近 24 小时已采样 {len(recent_entries)} 次，暂未出现重复异常。"
    else:
        status_text = "趋势数据积累中，至少完成几轮后台抓取后再看更有意义。"
    return {
        "window_hours": 24,
        "sample_count": len(recent_entries),
        "issue_runs": recent_issue_runs,
        "repeat_count": len(repeat_items),
        "repeat_items": repeat_items,
        "status_text": status_text,
        "repeated_codes": dict(repeated_codes),
    }


def _build_trend_alert(trend: dict[str, object], alerts: list[dict[str, str]]) -> dict[str, str] | None:
    sample_count = max(int(trend.get("sample_count") or 0), 0)
    issue_runs = max(int(trend.get("issue_runs") or 0), 0)
    repeat_items = trend.get("repeat_items") if isinstance(trend.get("repeat_items"), list) else []
    if sample_count < 3 or issue_runs < 2 or not repeat_items:
        return None

    current_codes = {
        normalize_text(alert.get("code")).lower()
        for alert in alerts
        if isinstance(alert, dict) and normalize_text(alert.get("code"))
    }
    active_repeat_items = [
        item
        for item in repeat_items
        if normalize_text(item.get("code")).lower() in current_codes
    ]
    if not active_repeat_items:
        return None

    max_repeat_count = max(max(int(item.get("count") or 0), 0) for item in active_repeat_items)
    severity = "critical" if max_repeat_count >= 3 else "warn"
    repeat_text = "；".join(
        f"{normalize_text(item.get('title')) or normalize_text(item.get('code'))} {max(int(item.get('count') or 0), 0)} 次"
        for item in active_repeat_items
    )
    return _build_alert(
        severity,
        "trend_repeated_anomalies",
        "近 24 小时异常反复出现",
        f"近 24 小时共采样 {sample_count} 次，其中 {issue_runs} 次出现异常；当前仍在重复的信号有：{repeat_text}。",
        "优先处理重复次数最高的异常，不要只看当前这一轮是否偶发。",
    )


def build_observability_history_entry(status: dict[str, object], *, source: str = "runtime", now_ts: int | None = None) -> dict[str, object]:
    overview = status.get("overview") if isinstance(status.get("overview"), dict) else {}
    metrics = status.get("metrics") if isinstance(status.get("metrics"), dict) else {}
    source_mix = metrics.get("source_mix") if isinstance(metrics.get("source_mix"), dict) else {}
    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    opencli = metrics.get("opencli") if isinstance(metrics.get("opencli"), dict) else {}
    trend = metrics.get("trend") if isinstance(metrics.get("trend"), dict) else {}
    alerts = status.get("alerts") if isinstance(status.get("alerts"), list) else []
    alert_codes = [
        normalize_text(alert.get("code")).lower()
        for alert in alerts
        if (
            isinstance(alert, dict)
            and normalize_text(alert.get("code"))
            and normalize_text(alert.get("severity")).lower() in {"warn", "critical"}
        )
    ]
    captured_at = max(int(now_ts or time.time()), 0)
    return {
        "captured_at": captured_at,
        "source": normalize_text(source).lower() or "runtime",
        "level": normalize_text(overview.get("level")).lower() or "ok",
        "issue_count": max(len(alert_codes), 0),
        "alert_codes": alert_codes,
        "primary_code": alert_codes[0] if alert_codes else "",
        "payload_source": normalize_text(overview.get("payload_source")).lower(),
        "rss_share": round(float(source_mix.get("rss_share") or 0.0), 4),
        "fallback_ratio": round(float(summary.get("fallback_ratio") or 0.0), 4),
        "x_total_configured": max(int(source_mix.get("x_total_configured") or 0), 0),
        "opencli_state": normalize_text(opencli.get("state")).lower(),
        "repeat_codes": dict(trend.get("repeated_codes") or {}) if isinstance(trend.get("repeated_codes"), dict) else {},
    }


def build_observability_status(
    *,
    cfg: dict[str, object] | None = None,
    latest_digest: dict[str, object] | None = None,
    scheduler_status: dict[str, object] | None = None,
    summary_status: dict[str, object] | None = None,
    task_snapshot: dict[str, object] | None = None,
    opencli_status: dict[str, object] | None = None,
    history_entries: list[dict[str, object]] | None = None,
    now_ts: int | None = None,
) -> dict[str, object]:
    cfg = cfg if isinstance(cfg, dict) else load_intel_config()
    latest_digest = latest_digest if isinstance(latest_digest, dict) else load_latest_digest()
    summary_status = summary_status if isinstance(summary_status, dict) else build_summary_status(cfg)
    scheduler = scheduler_status if isinstance(scheduler_status, dict) else {}
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else {}
    opencli = opencli_status if isinstance(opencli_status, dict) else {}
    history_entries = _coerce_history_entries(history_entries if isinstance(history_entries, list) else load_observability_history())
    current_ts = int(now_ts or time.time())

    active_payload, payload_source = _select_active_payload(latest_digest, task_snapshot)
    build_stats = active_payload.get("build_stats") if isinstance(active_payload.get("build_stats"), dict) else {}
    summary = summary_status.get("summary") if isinstance(summary_status.get("summary"), dict) else {}
    source_counts = _coerce_source_counts(build_stats)
    configured_x_topics = _configured_x_topics(cfg)
    configured_x_counts = _sum_source_counts(source_counts, configured_x_topics)
    overall_source_counts = _sum_source_counts(source_counts)
    summary_stats = build_stats.get("summary") if isinstance(build_stats.get("summary"), dict) else {}
    opencli_known = "installed" in opencli
    opencli_installed = bool(opencli.get("installed")) if opencli_known else False
    opencli_connected = bool(opencli.get("connected")) if opencli_known else False
    opencli_auto_recover = bool(opencli.get("auto_recover_on_demand")) if opencli_known else False
    opencli_status_stale = bool(opencli.get("status_stale")) if opencli_known else False
    opencli_connection_source = normalize_text(opencli.get("connection_source")).lower() if opencli_known else ""
    opencli_message = normalize_text(opencli.get("message")) if opencli_known else ""
    opencli_hint = normalize_text(opencli.get("hint")) if opencli_known else ""
    opencli_last_status_error = normalize_text(opencli.get("last_status_error")) if opencli_known else ""
    opencli_status_age_seconds = None
    if opencli_known:
        try:
            raw_age = opencli.get("status_age_seconds")
            opencli_status_age_seconds = max(int(raw_age), 0) if raw_age is not None else None
        except Exception:
            opencli_status_age_seconds = None
    opencli_state = "unknown"
    if opencli_known:
        if not opencli_installed:
            opencli_state = "missing"
        elif opencli_connected:
            opencli_state = "connected"
        elif opencli_auto_recover:
            opencli_state = "standby"
        elif opencli_status_stale:
            opencli_state = "stale"
        else:
            opencli_state = "disconnected"

    selected_counts = build_stats.get("selected_counts") if isinstance(build_stats.get("selected_counts"), dict) else {}
    selected_total = sum(max(int(value or 0), 0) for value in selected_counts.values())
    raw_counts = build_stats.get("raw_counts") if isinstance(build_stats.get("raw_counts"), dict) else {}
    raw_total = sum(max(int(value or 0), 0) for value in raw_counts.values())

    alerts: list[dict[str, str]] = []

    last_collect_error = normalize_text(scheduler.get("last_collect_error"))
    last_collect_at = normalize_text(scheduler.get("last_collect_at"))
    last_collect_message = normalize_text(scheduler.get("last_collect_message"))
    last_collect_ts = _parse_iso_ts(last_collect_at)
    collect_interval_seconds = max(int(scheduler.get("collect_interval_seconds") or 0), 1)
    if last_collect_error:
        alerts.append(
            _build_alert(
                "warn",
                "background_collect_failed",
                "后台抓取最近失败",
                f"调度器最近一次抓取报错：{last_collect_error}",
                "先看诊断信息和 opencli 状态，确认扩展与抓取链路是否恢复。",
            )
        )
    elif scheduler.get("running") and not last_collect_ts:
        alerts.append(
            _build_alert(
                "info" if last_collect_message in {"后台抓取中", "等待启动后的首次后台抓取"} else "warn",
                "background_collect_starting" if last_collect_message in {"后台抓取中", "等待启动后的首次后台抓取"} else "background_collect_missing",
                "后台抓取启动中" if last_collect_message in {"后台抓取中", "等待启动后的首次后台抓取"} else "后台抓取尚未完成首次运行",
                (
                    "服务刚启动，首轮后台抓取尚未完成；在这段时间内，来源结构和诊断结论可能仍沿用旧缓存。"
                    if last_collect_message in {"后台抓取中", "等待启动后的首次后台抓取"}
                    else "调度器已经启动，但还没有最近一次抓取时间。"
                ),
                (
                    "通常无需刷新页面，等首轮抓取完成后会自动恢复正常。"
                    if last_collect_message in {"后台抓取中", "等待启动后的首次后台抓取"}
                    else "如果长时间不更新，优先检查调度器和后台抓取日志。"
                ),
            )
        )
    elif last_collect_ts and current_ts - last_collect_ts > max(int(collect_interval_seconds * 2.5), collect_interval_seconds + 1800):
        alerts.append(
            _build_alert(
                "warn",
                "background_collect_stale",
                "后台抓取更新偏慢",
                f"最近一次后台抓取时间是 {last_collect_at}，已经超过预期抓取间隔。",
                "优先检查调度器是否仍在运行，以及抓取是否被异常阻塞。",
            )
        )

    scheduler_delivery_overdue = bool(scheduler.get("delivery_overdue"))
    scheduler_delivery_overdue_minutes = max(int(scheduler.get("delivery_overdue_minutes") or 0), 0)
    scheduler_delivery_fallback_due = bool(scheduler.get("delivery_fallback_due"))
    scheduler_delivery_fallback_attempted = bool(scheduler.get("delivery_fallback_attempted"))
    scheduler_fallback_result_message = normalize_text(scheduler.get("fallback_result_message"))
    if scheduler_delivery_overdue:
        alerts.append(
            _build_alert(
                "critical" if scheduler_delivery_fallback_due or scheduler_delivery_fallback_attempted else "warn",
                "daily_delivery_overdue",
                "正式日报发送已超时",
                (
                    f"今日日报已超过计划发送时间 {scheduler_delivery_overdue_minutes} 分钟，且已执行过一次自动补偿。"
                    if scheduler_delivery_fallback_attempted
                    else (
                        f"今日日报已超过计划发送时间 {scheduler_delivery_overdue_minutes} 分钟，已达到自动补偿阈值。"
                        if scheduler_delivery_fallback_due
                        else f"今日日报已超过计划发送时间 {scheduler_delivery_overdue_minutes} 分钟，仍未确认发送。"
                    )
                ),
                scheduler_fallback_result_message or "优先检查 Telegram 连通、发送状态和当日待确认记录。",
            )
        )

    opencli_blocks_x = False
    if configured_x_topics and opencli_known:
        if not opencli_installed:
            opencli_blocks_x = True
            alerts.append(
                _build_alert(
                    "critical",
                    "opencli_not_installed",
                    "已配置 X 主线，但本机未安装 opencli",
                    "当前策略依赖 X 作为主时效源，但运行环境没有可用 opencli，X 抓取会直接缺失。",
                    opencli_hint or "先安装 opencli，再恢复 X 主线抓取。",
                )
            )
        elif opencli_status_stale and not opencli_connected and not opencli_auto_recover:
            alerts.append(
                _build_alert(
                    "warn",
                    "opencli_status_stale",
                    "opencli 状态已过期",
                    (
                        f"当前诊断拿到的是 {opencli_status_age_seconds} 秒前的旧状态，"
                        "暂时无法确认浏览器桥接是否已经恢复。"
                    ) if opencli_status_age_seconds is not None else "当前诊断拿到的是旧的 opencli 状态，暂时无法确认浏览器桥接是否已经恢复。",
                    opencli_last_status_error or opencli_hint or "可点击“检查 opencli”刷新状态，再确认 X 抓取是否恢复。",
                )
            )
        elif not opencli_connected and not opencli_auto_recover:
            opencli_blocks_x = True
            detail = "已检测到 opencli，但浏览器扩展当前未连通，X 主线抓取会直接受影响。"
            if opencli_last_status_error:
                detail = f"{detail} 最近错误：{opencli_last_status_error}"
            alerts.append(
                _build_alert(
                    "critical",
                    "opencli_bridge_disconnected",
                    "opencli 浏览器桥接未连通",
                    detail,
                    opencli_hint or "先恢复 opencli 扩展连接，再观察下一轮 X 抓取是否恢复。",
                )
            )

    if configured_x_topics and not opencli_blocks_x and configured_x_counts["total"] > 0 and configured_x_counts["x"] <= 0:
        alerts.append(
            _build_alert(
                "critical",
                "x_source_missing",
                "已配置 X 主线，但最近构建没有 X 候选",
                "当前主线候选基本不含 X，日报更可能被 RSS / Reddit 支撑。",
                "先检查 opencli 状态，再看是否是当前查询词或浏览器桥接异常。",
            )
        )

    rss_share = (overall_source_counts["rss"] / overall_source_counts["total"]) if overall_source_counts["total"] else 0.0
    if overall_source_counts["total"] >= 6 and rss_share >= 0.85:
        alerts.append(
            _build_alert(
                "warn",
                "rss_dominant",
                "当前候选明显偏向 RSS",
                f"最近一次构建中 RSS 占比约 {round(rss_share * 100)}%，来源结构偏单一。",
                "如果连续多轮都这样，优先排查 X 抓取与查询词质量，而不是继续调排序权重。",
            )
        )

    if summary.get("mode") == "ai_first":
        if not bool(summary.get("ai_available")):
            alerts.append(
                _build_alert(
                    "warn",
                    "ai_summary_unavailable",
                    "AI 摘要当前不可用",
                    "摘要模式仍是 AI 优先，但当前没有可用 AI 凭证或运行时不可达。",
                    "如需恢复 AI 摘要，先检查本机 gmn/OpenAI 兼容接口和摘要配置。",
                )
            )
        fallback_ratio = _summary_fallback_ratio(summary_stats)
        total_items = max(int(summary_stats.get("total_items") or 0), 0)
        if total_items >= 3 and fallback_ratio >= 0.6:
            alerts.append(
                _build_alert(
                    "critical" if fallback_ratio >= 0.95 else "warn",
                    "summary_fallback_high",
                    "AI 摘要回退比例偏高",
                    f"最近一次构建共有 {total_items} 条摘要，其中回退 {max(int(summary_stats.get('fallback_items') or 0), 0)} 条。",
                    "如果这不是临时波动，优先检查摘要模型可用性、超时和批处理重试结果。",
                )
            )

    if raw_total > 0 and selected_total <= 0:
        alerts.append(
            _build_alert(
                "warn",
                "selection_empty",
                "候选存在，但最终日报为空",
                f"最近一次构建有 {raw_total} 条候选，但最终没有入选内容。",
                "优先检查 sent 去重、筛选门槛和累计事件池是否过严。",
            )
        )

    task = task_snapshot.get("task") if isinstance(task_snapshot.get("task"), dict) else {}
    task_status = normalize_text(task.get("status")).lower()
    task_updated_at = normalize_text(task.get("updated_at"))
    task_updated_ts = _parse_iso_ts(task_updated_at)
    if task_status == "failed" and task_updated_ts and current_ts - task_updated_ts <= 12 * 60 * 60:
        alerts.append(
            _build_alert(
                "warn",
                "build_task_failed_recently",
                "最近一次手动预览任务失败",
                normalize_text(task.get("error")) or normalize_text(task.get("message")) or "后台任务失败。",
                "如果需要立刻排查，可重新生成一次预览并观察任务状态与错误信息。",
            )
        )
    elif task_status in {"queued", "running"} and task_updated_ts and current_ts - task_updated_ts > 20 * 60:
        alerts.append(
            _build_alert(
                "warn",
                "build_task_stuck",
                "手动预览任务运行过久",
                f"任务状态 {task_status}，最后更新时间 {task_updated_at}。",
                "如果长时间不结束，优先检查 opencli 抓取或 AI 摘要是否卡住。",
            )
        )

    trend = _build_trend_summary(history_entries, alerts, now_ts=current_ts)
    trend_alert = _build_trend_alert(trend, alerts)
    if trend_alert:
        alerts.append(trend_alert)

    alerts.sort(key=lambda item: (_SEVERITY_RANK.get(item.get("severity", "ok"), 0), item.get("code", "")), reverse=True)
    level = alerts[0]["severity"] if alerts else "ok"
    status_text = (
        f"当前有 {len(alerts)} 个运行提示，最新状态是“{alerts[0]['title']}”。"
        if alerts and level == "info"
        else (
            f"发现 {len(alerts)} 个需要关注的运行信号，优先处理“{alerts[0]['title']}”。"
            if alerts
            else "当前运行链路整体正常，未发现需要立即处理的异常。"
        )
    )

    summary_total_items = max(int(summary_stats.get("total_items") or 0), 0)
    fallback_items = max(int(summary_stats.get("fallback_items") or 0), 0)
    fallback_ratio = _summary_fallback_ratio(summary_stats)
    return {
        "ok": True,
        "app_version": APP_VERSION,
        "overview": {
            "level": level,
            "issue_count": len(alerts),
            "status_text": status_text,
            "updated_at": _now_iso(current_ts),
            "payload_source": payload_source,
        },
        "metrics": {
            "configured_x_topics": sorted(configured_x_topics),
            "source_mix": {
                **overall_source_counts,
                "rss_share": round(rss_share, 4),
                "x_total_configured": configured_x_counts["x"],
                "configured_total": configured_x_counts["total"],
            },
            "summary": {
                "mode": normalize_text(summary.get("mode")),
                "ai_available": bool(summary.get("ai_available")),
                "active_mode": normalize_text(summary.get("active_mode")),
                "total_items": summary_total_items,
                "fallback_items": fallback_items,
                "fallback_ratio": round(fallback_ratio, 4),
            },
            "trend": trend,
            "opencli": {
                "known": opencli_known,
                "state": opencli_state,
                "installed": opencli_installed,
                "connected": opencli_connected,
                "auto_recover_on_demand": opencli_auto_recover,
                "status_stale": opencli_status_stale,
                "status_age_seconds": opencli_status_age_seconds,
                "connection_source": opencli_connection_source,
                "message": opencli_message,
                "hint": opencli_hint,
                "last_status_error": opencli_last_status_error,
                "last_success_at": opencli.get("last_success_at") if opencli_known else None,
                "chrome_window_count": opencli.get("chrome_window_count") if opencli_known else None,
            },
            "build": {
                "raw_total": raw_total,
                "selected_total": selected_total,
            },
            "scheduler": {
                "running": bool(scheduler.get("running")),
                "last_collect_at": last_collect_at,
                "last_collect_error": last_collect_error,
                "collect_interval_seconds": collect_interval_seconds,
                "next_collect_due_at": normalize_text(scheduler.get("next_collect_due_at")),
                "delivery_overdue": scheduler_delivery_overdue,
                "delivery_overdue_minutes": scheduler_delivery_overdue_minutes,
                "delivery_fallback_due": scheduler_delivery_fallback_due,
                "delivery_fallback_attempted": scheduler_delivery_fallback_attempted,
            },
            "task": {
                "status": task_status,
                "updated_at": task_updated_at,
            },
        },
        "alerts": alerts,
    }
