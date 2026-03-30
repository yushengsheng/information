from __future__ import annotations

import re
from dataclasses import dataclass, field

from monitor import Thresholds, format_window


@dataclass(slots=True)
class SessionConfig:
    symbols: list[str] = field(default_factory=list)
    windows_seconds: list[int] = field(default_factory=lambda: [300, 600])
    report_interval: int = 10
    baseline_minutes: int = 120
    runtime_seconds: int = 0
    thresholds: Thresholds = field(default_factory=Thresholds)

    def to_dict(self) -> dict[str, object]:
        return {
            "symbols": self.symbols,
            "symbols_text": ",".join(self.symbols),
            "windows_seconds": self.windows_seconds,
            "windows_minutes": [window // 60 for window in self.windows_seconds],
            "window_labels": [format_window(window) for window in self.windows_seconds],
            "report_interval": self.report_interval,
            "baseline_minutes": self.baseline_minutes,
            "runtime_seconds": self.runtime_seconds,
        }

    def clone(self, *, symbols: list[str] | None = None) -> "SessionConfig":
        return SessionConfig(
            symbols=list(self.symbols if symbols is None else symbols),
            windows_seconds=list(self.windows_seconds),
            report_interval=self.report_interval,
            baseline_minutes=self.baseline_minutes,
            runtime_seconds=self.runtime_seconds,
            thresholds=self.thresholds,
        )


def parse_symbols(value: object, *, allow_empty: bool = False) -> list[str]:
    if isinstance(value, str):
        raw_tokens = re.split(r"[\s,，;；]+", value.strip())
        items = [token for token in raw_tokens if token]
    elif isinstance(value, list):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        raise ValueError("币种列表请输入逗号分隔内容，例如 NIGHTUSDT,BTCUSDT")

    symbols: list[str] = []
    seen: set[str] = set()
    for item in items:
        symbol = item.upper()
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)

    if not symbols and not allow_empty:
        raise ValueError("至少填写一个币种")
    return symbols


def parse_windows_minutes(value: object) -> list[int]:
    if isinstance(value, str):
        tokens = [token.strip() for token in value.replace("，", ",").split(",")]
        items = [token for token in tokens if token]
    elif isinstance(value, list):
        items = value
    else:
        raise ValueError("持续时间请输入逗号分隔的分钟数，例如 5,10")

    windows_minutes: list[int] = []
    for item in items:
        minutes = int(item)
        if minutes <= 0:
            raise ValueError("持续时间必须是正整数分钟")
        windows_minutes.append(minutes)

    unique_seconds = sorted({minutes * 60 for minutes in windows_minutes})
    if not unique_seconds:
        raise ValueError("至少填写一个持续时间")
    return unique_seconds


def parse_runtime(value: object) -> int:
    if value in (None, "", 0, "0"):
        return 0
    runtime_seconds = int(value)
    if runtime_seconds < 0:
        raise ValueError("自动停止秒数不能小于 0")
    return runtime_seconds


def parse_config(payload: dict[str, object]) -> SessionConfig:
    report_interval = int(payload.get("report_interval", 10))
    if report_interval <= 0:
        raise ValueError("刷新间隔必须大于 0 秒")

    baseline_minutes = int(payload.get("baseline_minutes", 120))
    if baseline_minutes <= 0:
        raise ValueError("基线分钟数必须大于 0")

    return SessionConfig(
        symbols=parse_symbols(payload.get("symbols", ""), allow_empty=True),
        windows_seconds=parse_windows_minutes(payload.get("windows_minutes", [5, 10])),
        report_interval=report_interval,
        baseline_minutes=baseline_minutes,
        runtime_seconds=parse_runtime(payload.get("runtime_seconds", 0)),
    )
