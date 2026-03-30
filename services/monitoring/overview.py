from __future__ import annotations


def build_sessions(symbols: list[str], snapshots: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    return [{"symbol": symbol, "snapshot": snapshots.get(symbol)} for symbol in symbols]


def build_multi_symbol_overview(sessions: list[dict[str, object]]) -> dict[str, object]:
    populated = [session for session in sessions if session.get("snapshot")]
    if not populated:
        return {
            "headline": "等待启动",
            "summary": "启动监控后，这里会汇总当前最强的币种信号。",
            "level": "warming",
            "strongest_symbol": None,
            "strongest_window": None,
            "symbol_count": len(sessions),
            "ready_count": 0,
            "phase_text": "等待启动",
        }

    priority = {"high": 4, "watch": 3, "active": 2, "low": 1, "warming": 0}
    strongest = max(
        populated,
        key=lambda item: (
            priority[item["snapshot"]["overall"]["level"]],
            item["snapshot"]["overall"]["window_label"] or "",
        ),
    )
    strongest_snapshot = strongest["snapshot"]
    strongest_overall = strongest_snapshot["overall"]

    return {
        "headline": strongest_overall["headline"],
        "summary": f"当前最强信号来自 {strongest['symbol']}，重点看 {strongest_overall['window_label']} 窗口。",
        "level": strongest_overall["level"],
        "strongest_symbol": strongest["symbol"],
        "strongest_window": strongest_overall["window_label"],
        "symbol_count": len(sessions),
        "ready_count": len(populated),
        "phase_text": f"已就绪 {len(populated)}/{len(sessions)} 个币种",
    }
