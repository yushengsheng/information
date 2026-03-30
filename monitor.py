#!/usr/bin/env python3
"""Binance spot taker-flow monitor for wash-like volume detection."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import signal
import ssl
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Deque, Iterable, Optional

import websockets

REST_BASES = (
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://data-api.binance.vision",
)
REST_BASE = REST_BASES[0]
WS_BASE = "wss://stream.binance.com:9443/stream?streams="
LogFn = Callable[[str], None]
DEFAULT_DEPTH_BAND_FRACTION = 0.002
DEFAULT_SLIPPAGE_REFERENCE_QUOTE = 10_000.0
_preferred_rest_base = REST_BASE
_rest_base_lock = threading.Lock()


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def format_number(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def format_ratio(value: float) -> str:
    if math.isinf(value):
        return "inf"
    return f"{value:.2f}x"


def format_duration(seconds: float) -> str:
    total = int(max(seconds, 0))
    minutes, sec = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}h{minutes:02d}m{sec:02d}s"
    if minutes:
        return f"{minutes:d}m{sec:02d}s"
    return f"{sec:d}s"


def format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def format_scalar(value: float, digits: int = 2) -> str:
    if math.isinf(value):
        return "inf"
    return f"{value:.{digits}f}"


def format_window(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def format_clock(ts: Optional[float] = None) -> str:
    when = datetime.fromtimestamp(ts or time.time()).astimezone()
    return when.strftime("%Y-%m-%d %H:%M:%S %z")


def emit_log(message: str, logger: Optional[LogFn] = None) -> None:
    if logger is None:
        print(message, flush=True)
        return
    logger(message)


def nice_step(value: float) -> float:
    if value <= 0:
        return 1.0
    exponent = math.floor(math.log10(value))
    scaled = value / (10**exponent)
    if scaled < 1.5:
        nice = 1.0
    elif scaled < 3.5:
        nice = 2.0
    elif scaled < 7.5:
        nice = 5.0
    else:
        nice = 10.0
    return nice * (10**exponent)


def pick_quote_bucket_size(avg_quote: float) -> float:
    if avg_quote <= 0:
        return 1.0
    base = avg_quote * 0.02
    if avg_quote >= 50:
        minimum = 1.0
    elif avg_quote >= 5:
        minimum = 0.1
    else:
        minimum = 0.01
    return max(minimum, nice_step(base))


def format_bucket_value(value: float, step: float) -> str:
    if step >= 1:
        return f"{value:.0f}"
    decimals = max(0, min(8, int(math.ceil(-math.log10(step)))))
    return f"{value:.{decimals}f}"


@dataclass(slots=True)
class TradeEvent:
    ts: float
    side: str
    price: float
    qty: float
    qty_text: str
    quote_qty: float


@dataclass(slots=True)
class MidSample:
    ts: float
    mid: float


@dataclass(slots=True)
class BaselineStats:
    median_quote_volume_1m: float
    mean_quote_volume_1m: float
    median_trade_count_1m: float
    sample_count: int


@dataclass(slots=True)
class SymbolInfo:
    symbol: str
    base_asset: str
    quote_asset: str
    tick_size: float
    step_size: float


@dataclass(slots=True)
class DepthInsight:
    is_ready: bool
    depth_band_fraction: float
    depth_bid_quote: float
    depth_ask_quote: float
    depth_min_quote: float
    slippage_reference_quote: float
    slippage_buy: Optional[float]
    slippage_sell: Optional[float]
    bid_levels: int
    ask_levels: int


@dataclass(slots=True)
class DepthSample:
    ts: float
    depth_min_quote: float
    max_slippage: float


@dataclass(slots=True)
class Thresholds:
    active_volume_ratio: float = 1.40
    active_trade_ratio: float = 1.15
    active_balance: float = 0.68
    active_switch_rate: float = 0.30
    suspicious_volume_ratio: float = 2.20
    suspicious_trade_ratio: float = 1.60
    suspicious_balance: float = 0.82
    suspicious_switch_rate: float = 0.45
    suspicious_max_return: float = 0.0040


@dataclass(slots=True)
class WindowMetrics:
    window_seconds: int
    state: str
    score: float
    streak_seconds: float
    trade_count: int
    total_quote: float
    buy_quote: float
    sell_quote: float
    balance: float
    switch_rate: float
    price_return: float
    volume_ratio: float
    trade_ratio: float
    churn_per_second: float
    refill_per_minute: float
    vol_to_depth_ratio: float
    vol_to_twap_depth_ratio: float
    twap_depth_quote: float
    twap_slippage: float
    interval_cv: float
    top_size_repeat_rate: float
    top5_size_repeat_rate: float
    top_size_bucket: str
    top_notional_repeat_rate: float
    top5_notional_repeat_rate: float
    top_notional_bucket: str


class BaselineTracker:
    def __init__(self, max_minutes: int) -> None:
        self._lock = threading.RLock()
        self.quote_volumes: Deque[float] = deque(maxlen=max_minutes)
        self.trade_counts: Deque[int] = deque(maxlen=max_minutes)

    def seed_from_klines(self, klines: Iterable[list[object]]) -> None:
        with self._lock:
            for kline in klines:
                quote_volume = float(kline[7])
                trade_count = int(kline[8])
                self.quote_volumes.append(quote_volume)
                self.trade_counts.append(trade_count)

    def update_from_closed_kline(self, quote_volume: float, trade_count: int) -> None:
        with self._lock:
            self.quote_volumes.append(quote_volume)
            self.trade_counts.append(trade_count)

    def stats(self) -> Optional[BaselineStats]:
        with self._lock:
            if not self.quote_volumes or not self.trade_counts:
                return None
            return BaselineStats(
                median_quote_volume_1m=statistics.median(self.quote_volumes),
                mean_quote_volume_1m=statistics.fmean(self.quote_volumes),
                median_trade_count_1m=statistics.median(self.trade_counts),
                sample_count=len(self.quote_volumes),
            )


class OrderBookSyncError(RuntimeError):
    """Raised when depth diff events can no longer be applied safely."""


class LocalOrderBook:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id = 0
        self.is_ready = False

    def bootstrap(self, snapshot: dict[str, object]) -> None:
        self.bids = {}
        self.asks = {}
        self._apply_updates(self.bids, snapshot.get("bids", []))
        self._apply_updates(self.asks, snapshot.get("asks", []))
        self.last_update_id = int(snapshot["lastUpdateId"])
        self.is_ready = True

    def apply_event(self, data: dict[str, object]) -> None:
        if not self.is_ready:
            return
        first_update_id = int(data["U"])
        final_update_id = int(data["u"])
        if final_update_id <= self.last_update_id:
            return
        if first_update_id > self.last_update_id + 1:
            raise OrderBookSyncError(
                f"depth diff gap detected: first={first_update_id}, local={self.last_update_id}"
            )
        self._apply_updates(self.bids, data.get("b", []))
        self._apply_updates(self.asks, data.get("a", []))
        self.last_update_id = final_update_id

    def snapshot_metrics(
        self,
        depth_band_fraction: float = DEFAULT_DEPTH_BAND_FRACTION,
        slippage_reference_quote: float = DEFAULT_SLIPPAGE_REFERENCE_QUOTE,
    ) -> Optional[DepthInsight]:
        if not self.is_ready or not self.bids or not self.asks:
            return None
        best_bid = max(self.bids)
        best_ask = min(self.asks)
        if best_bid <= 0 or best_ask <= 0:
            return None
        mid = (best_bid + best_ask) / 2.0
        lower_bound = mid * (1.0 - depth_band_fraction)
        upper_bound = mid * (1.0 + depth_band_fraction)

        depth_bid_quote = 0.0
        for price, qty in self._sorted_bids():
            if price < lower_bound:
                break
            depth_bid_quote += price * qty

        depth_ask_quote = 0.0
        for price, qty in self._sorted_asks():
            if price > upper_bound:
                break
            depth_ask_quote += price * qty

        return DepthInsight(
            is_ready=True,
            depth_band_fraction=depth_band_fraction,
            depth_bid_quote=depth_bid_quote,
            depth_ask_quote=depth_ask_quote,
            depth_min_quote=min(depth_bid_quote, depth_ask_quote),
            slippage_reference_quote=slippage_reference_quote,
            slippage_buy=self._estimate_buy_slippage(slippage_reference_quote, best_ask),
            slippage_sell=self._estimate_sell_slippage(slippage_reference_quote, best_bid),
            bid_levels=len(self.bids),
            ask_levels=len(self.asks),
        )

    def _apply_updates(self, side: dict[float, float], updates: Iterable[Iterable[object]]) -> None:
        for price_raw, qty_raw in updates:
            price = float(price_raw)
            qty = float(qty_raw)
            if qty == 0.0:
                side.pop(price, None)
            else:
                side[price] = qty

    def _sorted_bids(self) -> list[tuple[float, float]]:
        return sorted(self.bids.items(), reverse=True)

    def _sorted_asks(self) -> list[tuple[float, float]]:
        return sorted(self.asks.items())

    def _estimate_buy_slippage(self, target_quote: float, best_ask: float) -> Optional[float]:
        remaining_quote = target_quote
        spent_quote = 0.0
        filled_base = 0.0
        for price, qty in self._sorted_asks():
            level_quote = price * qty
            take_quote = min(remaining_quote, level_quote)
            if take_quote <= 0:
                continue
            take_base = take_quote / price
            spent_quote += take_quote
            filled_base += take_base
            remaining_quote -= take_quote
            if remaining_quote <= 1e-9:
                average_price = spent_quote / filled_base
                return max(0.0, average_price / best_ask - 1.0)
        return None

    def _estimate_sell_slippage(self, target_quote: float, best_bid: float) -> Optional[float]:
        remaining_quote = target_quote
        received_quote = 0.0
        sold_base = 0.0
        for price, qty in self._sorted_bids():
            level_quote = price * qty
            take_quote = min(remaining_quote, level_quote)
            if take_quote <= 0:
                continue
            take_base = take_quote / price
            received_quote += take_quote
            sold_base += take_base
            remaining_quote -= take_quote
            if remaining_quote <= 1e-9:
                average_price = received_quote / sold_base
                return max(0.0, 1.0 - average_price / best_bid)
        return None


class WashVolumeMonitor:
    def __init__(
        self,
        symbol: str,
        symbol_info: SymbolInfo,
        windows: list[int],
        thresholds: Thresholds,
        baseline_tracker: BaselineTracker,
    ) -> None:
        self._lock = threading.RLock()
        self.symbol = symbol.upper()
        self.symbol_info = symbol_info
        self.windows = sorted(set(windows))
        self.max_window = max(self.windows)
        self.thresholds = thresholds
        self.baseline_tracker = baseline_tracker
        self.depth_band_fraction = DEFAULT_DEPTH_BAND_FRACTION
        self.slippage_reference_quote = DEFAULT_SLIPPAGE_REFERENCE_QUOTE
        self.depth_sample_horizon = max(self.max_window, 3600)

        self.trades: Deque[TradeEvent] = deque()
        self.book_update_ts: Deque[float] = deque()
        self.mid_samples: Deque[MidSample] = deque()
        self.refill_ts: Deque[float] = deque()
        self.depth_samples: Deque[DepthSample] = deque()
        self.order_book = LocalOrderBook()

        self.last_book: Optional[tuple[float, float, float, float]] = None
        self.last_mid_sample_ts = 0.0
        self.last_depth_sample_ts = 0.0
        self.live_started_at = 0.0
        self.window_state_since = {window: 0.0 for window in self.windows}
        self.window_state_label = {window: "BOOTSTRAP" for window in self.windows}

    def on_trade(self, data: dict[str, object]) -> None:
        with self._lock:
            ts = float(data["T"]) / 1000.0
            if self.live_started_at == 0.0:
                self.live_started_at = ts
            price = float(data["p"])
            qty_text = str(data["q"])
            qty = float(qty_text)
            quote_qty = price * qty
            side = "BUY" if not bool(data["m"]) else "SELL"
            self.trades.append(
                TradeEvent(
                    ts=ts,
                    side=side,
                    price=price,
                    qty=qty,
                    qty_text=qty_text,
                    quote_qty=quote_qty,
                )
            )
            self._prune(ts)

    def on_book(self, data: dict[str, object]) -> None:
        with self._lock:
            ts = time.time()
            if self.live_started_at == 0.0:
                self.live_started_at = ts
            bid = float(data["b"])
            bid_qty = float(data["B"])
            ask = float(data["a"])
            ask_qty = float(data["A"])
            self.book_update_ts.append(ts)

            if self.last_book is not None:
                prev_bid, prev_bid_qty, prev_ask, prev_ask_qty = self.last_book
                bid_notional_increase = max(0.0, bid_qty - prev_bid_qty) * bid
                ask_notional_increase = max(0.0, ask_qty - prev_ask_qty) * ask
                same_price_bid_refill = math.isclose(bid, prev_bid, rel_tol=0.0, abs_tol=1e-12)
                same_price_ask_refill = math.isclose(ask, prev_ask, rel_tol=0.0, abs_tol=1e-12)
                if same_price_bid_refill and bid_notional_increase >= 1_000:
                    self.refill_ts.append(ts)
                if same_price_ask_refill and ask_notional_increase >= 1_000:
                    self.refill_ts.append(ts)

            self.last_book = (bid, bid_qty, ask, ask_qty)
            self._sample_market_state(ts)
            self._prune(ts)

    def on_closed_kline(self, data: dict[str, object]) -> None:
        with self._lock:
            kline = data["k"]
            if not bool(kline["x"]):
                return
            self.baseline_tracker.update_from_closed_kline(
                quote_volume=float(kline["q"]),
                trade_count=int(kline["n"]),
            )

    def reset_depth_book(self) -> None:
        with self._lock:
            self.order_book.reset()

    def bootstrap_depth_book(self, snapshot: dict[str, object]) -> None:
        with self._lock:
            self.order_book.bootstrap(snapshot)

    def on_depth(self, data: dict[str, object]) -> None:
        with self._lock:
            self.order_book.apply_event(data)
            self._sample_market_state(time.time())

    def _prune(self, now_ts: float) -> None:
        keep_after = now_ts - self.depth_sample_horizon - 120.0
        while self.trades and self.trades[0].ts < keep_after:
            self.trades.popleft()
        while self.book_update_ts and self.book_update_ts[0] < keep_after:
            self.book_update_ts.popleft()
        while self.mid_samples and self.mid_samples[0].ts < keep_after:
            self.mid_samples.popleft()
        while self.refill_ts and self.refill_ts[0] < keep_after:
            self.refill_ts.popleft()
        while self.depth_samples and self.depth_samples[0].ts < keep_after:
            self.depth_samples.popleft()

    def _sample_market_state(self, ts: float) -> None:
        if self.last_book is not None and ts - self.last_mid_sample_ts >= 1.0:
            mid = (self.last_book[0] + self.last_book[2]) / 2.0
            self.mid_samples.append(MidSample(ts=ts, mid=mid))
            self.last_mid_sample_ts = ts
        if ts - self.last_depth_sample_ts >= 1.0:
            depth_insight = self.order_book.snapshot_metrics(
                depth_band_fraction=self.depth_band_fraction,
                slippage_reference_quote=self.slippage_reference_quote,
            )
            if depth_insight is not None:
                self.depth_samples.append(
                    DepthSample(
                        ts=ts,
                        depth_min_quote=depth_insight.depth_min_quote,
                        max_slippage=max(depth_insight.slippage_buy or 0.0, depth_insight.slippage_sell or 0.0),
                    )
                )
                self.last_depth_sample_ts = ts

    def evaluate_window(self, window_seconds: int, now_ts: Optional[float] = None) -> WindowMetrics:
        with self._lock:
            now_ts = now_ts or time.time()
            cutoff = now_ts - window_seconds

            trades = [trade for trade in self.trades if trade.ts >= cutoff]
            trade_count = len(trades)
            total_quote = sum(trade.quote_qty for trade in trades)
            buy_quote = sum(trade.quote_qty for trade in trades if trade.side == "BUY")
            sell_quote = total_quote - buy_quote
            balance = (
                min(buy_quote, sell_quote) / max(buy_quote, sell_quote)
                if max(buy_quote, sell_quote, 0.0)
                else 0.0
            )
            switches = sum(1 for index in range(1, trade_count) if trades[index].side != trades[index - 1].side)
            switch_rate = switches / max(trade_count - 1, 1)

            baseline = self.baseline_tracker.stats()
            if baseline is None:
                baseline_quote = 0.0
                baseline_trades = 0.0
            else:
                baseline_quote = baseline.median_quote_volume_1m * (window_seconds / 60.0)
                baseline_trades = baseline.median_trade_count_1m * (window_seconds / 60.0)

            volume_ratio = (total_quote / baseline_quote) if baseline_quote > 0 else math.inf
            trade_ratio = (trade_count / baseline_trades) if baseline_trades > 0 else math.inf

            mids = [sample for sample in self.mid_samples if sample.ts >= cutoff]
            if len(mids) >= 2:
                start_mid = mids[0].mid
                end_mid = mids[-1].mid
            elif trade_count >= 2:
                start_mid = trades[0].price
                end_mid = trades[-1].price
            elif self.last_book is not None:
                start_mid = (self.last_book[0] + self.last_book[2]) / 2.0
                end_mid = start_mid
            else:
                start_mid = 0.0
                end_mid = 0.0
            price_return = abs((end_mid - start_mid) / start_mid) if start_mid > 0 else 0.0

            churn_events = sum(1 for ts in self.book_update_ts if ts >= cutoff)
            churn_per_second = churn_events / window_seconds
            refill_events = sum(1 for ts in self.refill_ts if ts >= cutoff)
            refill_per_minute = refill_events * 60.0 / window_seconds
            depth_insight = self.order_book.snapshot_metrics(
                depth_band_fraction=self.depth_band_fraction,
                slippage_reference_quote=self.slippage_reference_quote,
            )
            if depth_insight is not None and depth_insight.depth_min_quote > 0:
                vol_to_depth_ratio = total_quote / depth_insight.depth_min_quote
            else:
                vol_to_depth_ratio = 0.0

            depth_samples = [sample for sample in self.depth_samples if sample.ts >= cutoff]
            if depth_samples:
                twap_depth_quote = statistics.median(sample.depth_min_quote for sample in depth_samples)
                twap_slippage = statistics.median(sample.max_slippage for sample in depth_samples)
            else:
                twap_depth_quote = 0.0
                twap_slippage = 0.0
            vol_to_twap_depth_ratio = (total_quote / twap_depth_quote) if twap_depth_quote > 0 else 0.0

            if trade_count >= 6:
                intervals = [trades[index].ts - trades[index - 1].ts for index in range(1, trade_count)]
                mean_interval = statistics.fmean(intervals)
                if mean_interval > 0 and len(intervals) > 1:
                    interval_cv = statistics.pstdev(intervals) / mean_interval
                else:
                    interval_cv = 1.0
            else:
                interval_cv = 1.0

            if trade_count > 0:
                qty_counts = Counter(trade.qty_text for trade in trades)
                most_common = qty_counts.most_common(5)
                top_size_bucket = most_common[0][0]
                top_size_repeat_rate = most_common[0][1] / trade_count
                top5_size_repeat_rate = sum(count for _, count in most_common) / trade_count

                quote_bucket_step = pick_quote_bucket_size(total_quote / trade_count)
                notional_counts = Counter(
                    round(trade.quote_qty / quote_bucket_step) * quote_bucket_step for trade in trades
                )
                notional_common = notional_counts.most_common(5)
                top_notional_bucket = format_bucket_value(notional_common[0][0], quote_bucket_step)
                top_notional_repeat_rate = notional_common[0][1] / trade_count
                top5_notional_repeat_rate = sum(count for _, count in notional_common) / trade_count
            else:
                top_size_bucket = "-"
                top_size_repeat_rate = 0.0
                top5_size_repeat_rate = 0.0
                top_notional_bucket = "-"
                top_notional_repeat_rate = 0.0
                top5_notional_repeat_rate = 0.0

            score = self._score(
                volume_ratio=volume_ratio,
                trade_ratio=trade_ratio,
                balance=balance,
                switch_rate=switch_rate,
                price_return=price_return,
                refill_per_minute=refill_per_minute,
                vol_to_depth_ratio=vol_to_depth_ratio,
                vol_to_twap_depth_ratio=vol_to_twap_depth_ratio,
                depth_insight=depth_insight,
                twap_slippage=twap_slippage,
                interval_cv=interval_cv,
                top_size_repeat_rate=top_size_repeat_rate,
                top5_size_repeat_rate=top5_size_repeat_rate,
                top_notional_repeat_rate=top_notional_repeat_rate,
                top5_notional_repeat_rate=top5_notional_repeat_rate,
            )
            live_age = now_ts - self.live_started_at if self.live_started_at else 0.0
            if live_age < window_seconds:
                state = "WARMING_UP"
                streak_seconds = self._update_streak(window_seconds, state, now_ts)
            else:
                state = self._classify(
                    volume_ratio=volume_ratio,
                    trade_ratio=trade_ratio,
                    balance=balance,
                    switch_rate=switch_rate,
                    price_return=price_return,
                )
                streak_seconds = self._update_streak(window_seconds, state, now_ts)

            return WindowMetrics(
                window_seconds=window_seconds,
                state=state,
                score=score,
                streak_seconds=streak_seconds,
                trade_count=trade_count,
                total_quote=total_quote,
                buy_quote=buy_quote,
                sell_quote=sell_quote,
                balance=balance,
                switch_rate=switch_rate,
                price_return=price_return,
                volume_ratio=volume_ratio,
                trade_ratio=trade_ratio,
                churn_per_second=churn_per_second,
                refill_per_minute=refill_per_minute,
                vol_to_depth_ratio=vol_to_depth_ratio,
                vol_to_twap_depth_ratio=vol_to_twap_depth_ratio,
                twap_depth_quote=twap_depth_quote,
                twap_slippage=twap_slippage,
                interval_cv=interval_cv,
                top_size_repeat_rate=top_size_repeat_rate,
                top5_size_repeat_rate=top5_size_repeat_rate,
                top_size_bucket=top_size_bucket,
                top_notional_repeat_rate=top_notional_repeat_rate,
                top5_notional_repeat_rate=top5_notional_repeat_rate,
                top_notional_bucket=top_notional_bucket,
            )

    def _classify(
        self,
        *,
        volume_ratio: float,
        trade_ratio: float,
        balance: float,
        switch_rate: float,
        price_return: float,
    ) -> str:
        thresholds = self.thresholds
        suspicious = (
            volume_ratio >= thresholds.suspicious_volume_ratio
            and trade_ratio >= thresholds.suspicious_trade_ratio
            and balance >= thresholds.suspicious_balance
            and switch_rate >= thresholds.suspicious_switch_rate
            and price_return <= thresholds.suspicious_max_return
        )
        if suspicious:
            return "SUSPECTED_WASH_LIKE"

        active = (
            volume_ratio >= thresholds.active_volume_ratio
            and trade_ratio >= thresholds.active_trade_ratio
            and balance >= thresholds.active_balance
            and switch_rate >= thresholds.active_switch_rate
        )
        if active:
            return "TWO_SIDED_ACTIVE"
        return "NORMAL"

    def _score(
        self,
        *,
        volume_ratio: float,
        trade_ratio: float,
        balance: float,
        switch_rate: float,
        price_return: float,
        refill_per_minute: float,
        vol_to_depth_ratio: float,
        vol_to_twap_depth_ratio: float,
        depth_insight: Optional[DepthInsight],
        twap_slippage: float,
        interval_cv: float,
        top_size_repeat_rate: float,
        top5_size_repeat_rate: float,
        top_notional_repeat_rate: float,
        top5_notional_repeat_rate: float,
    ) -> float:
        thresholds = self.thresholds
        volume_component = clamp(volume_ratio / thresholds.suspicious_volume_ratio, 0.0, 1.0)
        trade_component = clamp(trade_ratio / thresholds.suspicious_trade_ratio, 0.0, 1.0)
        balance_component = clamp(balance / thresholds.suspicious_balance, 0.0, 1.0)
        switch_component = clamp(switch_rate / thresholds.suspicious_switch_rate, 0.0, 1.0)
        impact_component = clamp(
            thresholds.suspicious_max_return / max(price_return, thresholds.suspicious_max_return),
            0.0,
            1.0,
        )
        refill_component = clamp(refill_per_minute / 3.0, 0.0, 1.0)
        depth_ratio_component = clamp(vol_to_depth_ratio / 4.0, 0.0, 1.0)
        twap_depth_ratio_component = clamp(vol_to_twap_depth_ratio / 4.0, 0.0, 1.0)
        max_slippage = 0.0
        if depth_insight is not None:
            max_slippage = max(depth_insight.slippage_buy or 0.0, depth_insight.slippage_sell or 0.0)
        slippage_component = clamp(max_slippage / 0.0030, 0.0, 1.0)
        twap_slippage_component = clamp(twap_slippage / 0.0030, 0.0, 1.0)
        depth_component = max(depth_ratio_component, twap_depth_ratio_component, slippage_component, twap_slippage_component)
        size_component = clamp(
            max(top_size_repeat_rate / 0.20, top5_size_repeat_rate / 0.55),
            0.0,
            1.0,
        )
        notional_component = clamp(
            max(top_notional_repeat_rate / 0.20, top5_notional_repeat_rate / 0.55),
            0.0,
            1.0,
        )
        interval_component = clamp((0.85 - interval_cv) / 0.85, 0.0, 1.0)
        base_score = (
            0.22 * volume_component
            + 0.15 * trade_component
            + 0.17 * balance_component
            + 0.12 * switch_component
            + 0.10 * impact_component
            + 0.10 * depth_component
            + 0.05 * refill_component
            + 0.05 * size_component
            + 0.02 * notional_component
            + 0.02 * interval_component
        )
        activity_gate = clamp((volume_component + trade_component) / 2.0, 0.0, 1.0)
        return base_score * activity_gate * 100.0

    def _update_streak(self, window_seconds: int, state: str, now_ts: float) -> float:
        previous_label = self.window_state_label[window_seconds]
        current_since = self.window_state_since[window_seconds]

        tracked = state in {"TWO_SIDED_ACTIVE", "SUSPECTED_WASH_LIKE"}
        if state != previous_label:
            self.window_state_label[window_seconds] = state
            self.window_state_since[window_seconds] = now_ts if tracked else 0.0
            return 0.0

        if not tracked:
            return 0.0
        if current_since == 0.0:
            self.window_state_since[window_seconds] = now_ts
            return 0.0
        return now_ts - current_since

    def render_report(self) -> str:
        snapshot = self.snapshot()
        lines = [
            "",
            f"[{snapshot['generated_at_text']}] symbol={snapshot['symbol']}",
            f"overall={snapshot['overall']['headline']} ({snapshot['overall']['window_label']})",
        ]
        baseline = snapshot["baseline"]
        depth = snapshot["depth"]
        if baseline is None:
            lines.append("baseline_1m=warming_up")
        else:
            lines.append(
                "baseline_1m="
                f"median_quote={baseline['median_quote_text']} USDT, "
                f"median_trades={baseline['median_trade_count_1m']:.0f}, "
                f"samples={baseline['sample_count']}"
            )
        if depth is None:
            lines.append("depth_book=syncing")
        else:
            lines.append(
                "depth_book="
                f"band={depth['depth_band_text']}, "
                f"bid_depth={depth['depth_bid_quote_text']} {self.symbol_info.quote_asset}, "
                f"ask_depth={depth['depth_ask_quote_text']} {self.symbol_info.quote_asset}, "
                f"slip@{depth['slippage_reference_quote_text']}="
                f"buy {depth['slippage_buy_text']} / sell {depth['slippage_sell_text']}"
            )

        for metrics in snapshot["windows"]:
            lines.append(
                f"{metrics['window_label']:>4} state={metrics['state']:<20} score={metrics['score']:5.1f} "
                f"streak={metrics['streak_text']:>8} verdict={metrics['judgement']['headline']}"
            )
            lines.append(
                "    "
                f"quote={metrics['total_quote_text']} ({metrics['volume_ratio_text']}), "
                f"buy={metrics['buy_quote_text']}, sell={metrics['sell_quote_text']}, "
                f"balance={metrics['balance']:.2f}"
            )
            lines.append(
                "    "
                f"trades={metrics['trade_count']} ({metrics['trade_ratio_text']}), "
                f"switch_rate={metrics['switch_rate']:.2f}, "
                f"move={metrics['price_return_text']}, "
                f"book_churn={metrics['churn_per_second']:.2f}/s, "
                f"refill={metrics['refill_per_minute']:.2f}/min"
            )
            lines.append(
                "    "
                f"vol/depth={metrics['vol_to_depth_ratio_text']}, "
                f"size_top1={metrics['top_size_repeat_text']} ({metrics['top_size_bucket']}), "
                f"size_top5={metrics['top5_size_repeat_text']}"
            )
            lines.append(
                "    "
                f"vol/tw_depth={metrics['vol_to_twap_depth_ratio_text']}, "
                f"tw_slip={metrics['twap_slippage_text']}, "
                f"gap_cv={metrics['interval_cv_text']}, "
                f"notional_top1={metrics['top_notional_repeat_text']} ({metrics['top_notional_bucket']})"
            )
            lines.append(
                "    "
                + " | ".join(
                    f"{axis['label']}={axis['status']}"
                    for axis in metrics["judgement"]["axes"]
                )
            )
        return "\n".join(lines)

    def snapshot(self, now_ts: Optional[float] = None) -> dict[str, object]:
        with self._lock:
            now_ts = now_ts or time.time()
            baseline = self.baseline_tracker.stats()
            live_age = now_ts - self.live_started_at if self.live_started_at else 0.0
            depth_insight = self.order_book.snapshot_metrics(
                depth_band_fraction=self.depth_band_fraction,
                slippage_reference_quote=self.slippage_reference_quote,
            )
            market = None
            if self.last_book is not None:
                best_bid, bid_qty, best_ask, ask_qty = self.last_book
                market = {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "bid_qty": bid_qty,
                    "ask_qty": ask_qty,
                    "mid": (best_bid + best_ask) / 2.0,
                }

            windows_payload = []
            for window in self.windows:
                metrics = self.evaluate_window(window, now_ts)
                metrics_dict = window_metrics_to_dict(metrics, live_age)
                metrics_dict["judgement"] = build_window_judgement(metrics, self.thresholds)
                windows_payload.append(metrics_dict)

            return {
                "symbol": self.symbol,
                "symbol_meta": {
                    "base_asset": self.symbol_info.base_asset,
                    "quote_asset": self.symbol_info.quote_asset,
                    "tick_size": self.symbol_info.tick_size,
                    "step_size": self.symbol_info.step_size,
                },
                "generated_at": now_ts,
                "generated_at_text": format_clock(now_ts),
                "live_age_seconds": live_age,
                "live_age_text": format_duration(live_age),
                "baseline": baseline_to_dict(baseline),
                "market": market,
                "depth": depth_insight_to_dict(depth_insight),
                "windows": windows_payload,
                "overall": build_overall_judgement(windows_payload),
            }


def baseline_to_dict(stats: Optional[BaselineStats]) -> Optional[dict[str, object]]:
    if stats is None:
        return None
    return {
        "median_quote_volume_1m": stats.median_quote_volume_1m,
        "median_quote_text": format_number(stats.median_quote_volume_1m),
        "mean_quote_volume_1m": stats.mean_quote_volume_1m,
        "mean_quote_text": format_number(stats.mean_quote_volume_1m),
        "median_trade_count_1m": stats.median_trade_count_1m,
        "sample_count": stats.sample_count,
    }


def depth_insight_to_dict(depth: Optional[DepthInsight]) -> Optional[dict[str, object]]:
    if depth is None:
        return None
    return {
        "is_ready": depth.is_ready,
        "depth_band_fraction": depth.depth_band_fraction,
        "depth_band_text": format_percent(depth.depth_band_fraction),
        "depth_bid_quote": depth.depth_bid_quote,
        "depth_bid_quote_text": format_number(depth.depth_bid_quote),
        "depth_ask_quote": depth.depth_ask_quote,
        "depth_ask_quote_text": format_number(depth.depth_ask_quote),
        "depth_min_quote": depth.depth_min_quote,
        "depth_min_quote_text": format_number(depth.depth_min_quote),
        "slippage_reference_quote": depth.slippage_reference_quote,
        "slippage_reference_quote_text": format_number(depth.slippage_reference_quote),
        "slippage_buy": depth.slippage_buy,
        "slippage_buy_text": format_percent(depth.slippage_buy or 0.0) if depth.slippage_buy is not None else "n/a",
        "slippage_sell": depth.slippage_sell,
        "slippage_sell_text": format_percent(depth.slippage_sell or 0.0) if depth.slippage_sell is not None else "n/a",
        "bid_levels": depth.bid_levels,
        "ask_levels": depth.ask_levels,
    }


def window_metrics_to_dict(metrics: WindowMetrics, live_age_seconds: float) -> dict[str, object]:
    return {
        "window_seconds": metrics.window_seconds,
        "window_label": format_window(metrics.window_seconds),
        "state": metrics.state,
        "score": metrics.score,
        "streak_seconds": metrics.streak_seconds,
        "streak_text": format_duration(metrics.streak_seconds),
        "trade_count": metrics.trade_count,
        "total_quote": metrics.total_quote,
        "total_quote_text": format_number(metrics.total_quote),
        "buy_quote": metrics.buy_quote,
        "buy_quote_text": format_number(metrics.buy_quote),
        "sell_quote": metrics.sell_quote,
        "sell_quote_text": format_number(metrics.sell_quote),
        "balance": metrics.balance,
        "switch_rate": metrics.switch_rate,
        "price_return": metrics.price_return,
        "price_return_text": format_percent(metrics.price_return),
        "volume_ratio": metrics.volume_ratio,
        "volume_ratio_text": format_ratio(metrics.volume_ratio),
        "trade_ratio": metrics.trade_ratio,
        "trade_ratio_text": format_ratio(metrics.trade_ratio),
        "churn_per_second": metrics.churn_per_second,
        "refill_per_minute": metrics.refill_per_minute,
        "vol_to_depth_ratio": metrics.vol_to_depth_ratio,
        "vol_to_depth_ratio_text": format_ratio(metrics.vol_to_depth_ratio),
        "vol_to_twap_depth_ratio": metrics.vol_to_twap_depth_ratio,
        "vol_to_twap_depth_ratio_text": format_ratio(metrics.vol_to_twap_depth_ratio),
        "twap_depth_quote": metrics.twap_depth_quote,
        "twap_depth_quote_text": format_number(metrics.twap_depth_quote),
        "twap_slippage": metrics.twap_slippage,
        "twap_slippage_text": format_percent(metrics.twap_slippage),
        "interval_cv": metrics.interval_cv,
        "interval_cv_text": format_scalar(metrics.interval_cv),
        "top_size_repeat_rate": metrics.top_size_repeat_rate,
        "top_size_repeat_text": format_percent(metrics.top_size_repeat_rate),
        "top5_size_repeat_rate": metrics.top5_size_repeat_rate,
        "top5_size_repeat_text": format_percent(metrics.top5_size_repeat_rate),
        "top_size_bucket": metrics.top_size_bucket,
        "top_notional_repeat_rate": metrics.top_notional_repeat_rate,
        "top_notional_repeat_text": format_percent(metrics.top_notional_repeat_rate),
        "top5_notional_repeat_rate": metrics.top5_notional_repeat_rate,
        "top5_notional_repeat_text": format_percent(metrics.top5_notional_repeat_rate),
        "top_notional_bucket": metrics.top_notional_bucket,
        "warmup_progress": clamp(live_age_seconds / metrics.window_seconds, 0.0, 1.0),
    }


def build_axis_result(
    key: str,
    label: str,
    status: str,
    score: float,
    summary: str,
) -> dict[str, object]:
    return {
        "key": key,
        "label": label,
        "status": status,
        "score": score,
        "summary": summary,
    }


def build_window_judgement(metrics: WindowMetrics, thresholds: Thresholds) -> dict[str, object]:
    if metrics.state == "WARMING_UP":
        axes = [
            build_axis_result("two_sided", "双边都放大", "warming", 0.0, "窗口还没跑满"),
            build_axis_result("price_stuck", "价格推不动", "warming", 0.0, "等待完整窗口"),
            build_axis_result("mechanical", "行为很机械", "warming", 0.0, "等待完整窗口"),
        ]
        return {
            "headline": "窗口预热中",
            "level": "warming",
            "summary": "先累积满这个窗口，再判断是否像刷量。",
            "positive_axes": 0,
            "axes": axes,
        }

    two_sided_score = (
        0.4 * clamp(metrics.volume_ratio / thresholds.suspicious_volume_ratio, 0.0, 1.0)
        + 0.3 * clamp(metrics.trade_ratio / thresholds.suspicious_trade_ratio, 0.0, 1.0)
        + 0.3 * clamp(metrics.balance / thresholds.suspicious_balance, 0.0, 1.0)
    )
    two_sided_positive = (
        metrics.volume_ratio >= thresholds.active_volume_ratio
        and metrics.trade_ratio >= thresholds.active_trade_ratio
        and metrics.balance >= thresholds.active_balance
    )
    if two_sided_positive:
        two_sided_status = "positive"
    elif metrics.volume_ratio >= thresholds.active_volume_ratio * 0.75 and metrics.balance >= 0.55:
        two_sided_status = "neutral"
    else:
        two_sided_status = "negative"
    two_sided_axis = build_axis_result(
        "two_sided",
        "双边都放大",
        two_sided_status,
        two_sided_score,
        f"量 {format_ratio(metrics.volume_ratio)} / 笔数 {format_ratio(metrics.trade_ratio)} / 平衡 {metrics.balance:.2f}",
    )

    price_score = clamp(
        (
            0.7 * (1.0 - clamp(metrics.price_return / max(thresholds.suspicious_max_return, 1e-12), 0.0, 1.0))
            + 0.3 * (1.0 - clamp(metrics.twap_slippage / 0.0030, 0.0, 1.0))
        ),
        0.0,
        1.0,
    )
    if two_sided_positive and metrics.price_return <= thresholds.suspicious_max_return:
        price_status = "positive"
    elif metrics.price_return <= thresholds.suspicious_max_return * 1.8:
        price_status = "neutral"
    else:
        price_status = "negative"
    price_axis = build_axis_result(
        "price_stuck",
        "价格推不动",
        price_status,
        price_score,
        f"位移 {format_percent(metrics.price_return)} / 时间加权滑点 {format_percent(metrics.twap_slippage)}",
    )

    mechanical_score = clamp(
        0.35 * clamp(metrics.switch_rate / thresholds.suspicious_switch_rate, 0.0, 1.0)
        + 0.30 * clamp((1.0 - metrics.interval_cv) / 0.85, 0.0, 1.0)
        + 0.20 * clamp(metrics.top_notional_repeat_rate / 0.20, 0.0, 1.0)
        + 0.15 * clamp(metrics.top_size_repeat_rate / 0.20, 0.0, 1.0),
        0.0,
        1.0,
    )
    if (
        metrics.switch_rate >= thresholds.active_switch_rate
        and metrics.interval_cv <= 0.95
        and (metrics.top_notional_repeat_rate >= 0.20 or metrics.top_size_repeat_rate >= 0.20)
    ):
        mechanical_status = "positive"
    elif (
        metrics.switch_rate >= thresholds.active_switch_rate
        and (metrics.interval_cv <= 1.10 or metrics.top_notional_repeat_rate >= 0.12)
    ):
        mechanical_status = "neutral"
    else:
        mechanical_status = "negative"
    mechanical_axis = build_axis_result(
        "mechanical",
        "行为很机械",
        mechanical_status,
        mechanical_score,
        f"切换率 {metrics.switch_rate:.2f} / 间隔CV {format_scalar(metrics.interval_cv)} / 名义Top1 {format_percent(metrics.top_notional_repeat_rate)}",
    )

    axes = [two_sided_axis, price_axis, mechanical_axis]
    positive_axes = sum(1 for axis in axes if axis["status"] == "positive")
    neutral_axes = sum(1 for axis in axes if axis["status"] == "neutral")

    if positive_axes == 3:
        headline = "高度疑似刷量"
        level = "high"
        summary = "双边主动成交同时放大，价格推不动，而且行为很机械。"
    elif positive_axes == 2 and neutral_axes >= 1:
        headline = "中度疑似刷量"
        level = "watch"
        summary = "三项主判断里已有两项明显成立，剩下一项接近触发。"
    elif positive_axes >= 2:
        headline = "需要继续盯"
        level = "watch"
        summary = "主信号已经出现，但证据还不够整齐。"
    elif positive_axes == 1 or neutral_axes >= 2:
        headline = "活跃但不够像刷量"
        level = "active"
        summary = "有局部异常，但还没形成完整的刷量特征。"
    else:
        headline = "目前偏正常"
        level = "low"
        summary = "三项主判断暂时都不够强。"

    return {
        "headline": headline,
        "level": level,
        "summary": summary,
        "positive_axes": positive_axes,
        "axes": axes,
    }


def build_overall_judgement(windows: list[dict[str, object]]) -> dict[str, object]:
    if not windows:
        return {
            "headline": "等待数据",
            "level": "warming",
            "summary": "启动监控后会在这里汇总主要结论。",
            "window_label": "-",
            "phase_text": "等待启动",
        }

    priority = {"high": 4, "watch": 3, "active": 2, "low": 1, "warming": 0}
    strongest = max(windows, key=lambda item: (priority[item["judgement"]["level"]], item["score"]))
    judgement = strongest["judgement"]

    if any(item["judgement"]["level"] == "warming" for item in windows):
        phase_text = "部分窗口预热中"
    else:
        phase_text = "窗口已就绪"

    return {
        "headline": judgement["headline"],
        "level": judgement["level"],
        "summary": judgement["summary"],
        "window_label": strongest["window_label"],
        "phase_text": phase_text,
    }


def _ordered_rest_bases() -> list[str]:
    with _rest_base_lock:
        preferred = _preferred_rest_base
    return [preferred, *[base for base in REST_BASES if base != preferred]]


def _set_preferred_rest_base(base: str) -> None:
    global _preferred_rest_base
    with _rest_base_lock:
        _preferred_rest_base = base


def fetch_json(
    path: str,
    params: Optional[dict[str, object]] = None,
    *,
    timeout: float = 20.0,
    attempts_per_base: int = 2,
    logger: Optional[LogFn] = None,
) -> object:
    normalized_path = path if path.startswith("/") else f"/{path}"
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    ordered_bases = _ordered_rest_bases()
    last_error: Optional[BaseException] = None

    for base_index, base in enumerate(ordered_bases):
        url = f"{base}{normalized_path}{query}"
        for attempt in range(1, attempts_per_base + 1):
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "bn-wash-monitor/1.0",
                    "Accept": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                _set_preferred_rest_base(base)
                return payload
            except urllib.error.HTTPError as exc:
                if 400 <= exc.code < 500 and exc.code not in {408, 429}:
                    raise
                last_error = exc
            except (urllib.error.URLError, TimeoutError, OSError, ssl.SSLError, json.JSONDecodeError) as exc:
                last_error = exc

            should_log = logger is not None and (attempt < attempts_per_base or base_index + 1 < len(ordered_bases))
            if should_log:
                emit_log(
                    (
                        f"[{format_clock()}] warning=rest request failed "
                        f"base={base} path={normalized_path} attempt={attempt}/{attempts_per_base}: {last_error}"
                    ),
                    logger,
                )
            if attempt < attempts_per_base:
                time.sleep(min(1.2 * attempt, 2.5))

    assert last_error is not None
    raise RuntimeError(f"rest bootstrap failed for {normalized_path}: {last_error}")


def load_symbol_info(symbol: str, logger: Optional[LogFn] = None) -> SymbolInfo:
    payload = fetch_json("/api/v3/exchangeInfo", {"symbol": symbol.upper()}, logger=logger)
    symbols = payload.get("symbols", [])
    if not symbols:
        raise RuntimeError(f"symbol {symbol.upper()} not found on Binance spot exchangeInfo")
    symbol_payload = symbols[0]
    status = symbol_payload.get("status")
    if status != "TRADING":
        raise RuntimeError(f"symbol {symbol.upper()} status is {status}, not TRADING")
    filters = {item.get("filterType"): item for item in symbol_payload.get("filters", [])}
    price_filter = filters.get("PRICE_FILTER", {})
    lot_filter = filters.get("LOT_SIZE", {})
    return SymbolInfo(
        symbol=symbol_payload["symbol"],
        base_asset=symbol_payload.get("baseAsset", ""),
        quote_asset=symbol_payload.get("quoteAsset", "USDT"),
        tick_size=float(price_filter.get("tickSize", 0.0) or 0.0),
        step_size=float(lot_filter.get("stepSize", 0.0) or 0.0),
    )


def load_initial_klines(symbol: str, limit: int, logger: Optional[LogFn] = None) -> list[list[object]]:
    payload = fetch_json(
        "/api/v3/klines",
        {
            "symbol": symbol.upper(),
            "interval": "1m",
            "limit": min(max(limit, 1), 1000),
        },
        logger=logger,
    )
    if not isinstance(payload, list):
        raise RuntimeError("unexpected kline response")
    if payload and int(payload[-1][6]) / 1000.0 > time.time():
        payload = payload[:-1]
    return payload


def load_order_book_snapshot(symbol: str, limit: int = 1000, logger: Optional[LogFn] = None) -> dict[str, object]:
    payload = fetch_json(
        "/api/v3/depth",
        {
            "symbol": symbol.upper(),
            "limit": min(max(limit, 100), 5000),
        },
        logger=logger,
    )
    if not isinstance(payload, dict):
        raise RuntimeError("unexpected depth snapshot response")
    return payload


def build_thresholds_from_args(args: argparse.Namespace) -> Thresholds:
    return Thresholds(
        active_volume_ratio=args.active_volume_ratio,
        active_trade_ratio=args.active_trade_ratio,
        active_balance=args.active_balance,
        active_switch_rate=args.active_switch_rate,
        suspicious_volume_ratio=args.suspicious_volume_ratio,
        suspicious_trade_ratio=args.suspicious_trade_ratio,
        suspicious_balance=args.suspicious_balance,
        suspicious_switch_rate=args.suspicious_switch_rate,
        suspicious_max_return=args.suspicious_max_return,
    )


def create_monitor(
    symbol: str,
    windows: list[int],
    thresholds: Thresholds,
    baseline_minutes: int,
    logger: Optional[LogFn] = None,
) -> WashVolumeMonitor:
    emit_log(f"[{format_clock()}] checking symbol={symbol.upper()}", logger)
    symbol_info = load_symbol_info(symbol, logger=logger)
    emit_log(f"[{format_clock()}] loading baseline_klines={baseline_minutes}", logger)
    baseline_tracker = BaselineTracker(max_minutes=min(max(baseline_minutes, 1), 1000))
    baseline_tracker.seed_from_klines(load_initial_klines(symbol, baseline_minutes, logger=logger))
    return WashVolumeMonitor(
        symbol=symbol,
        symbol_info=symbol_info,
        windows=windows,
        thresholds=thresholds,
        baseline_tracker=baseline_tracker,
    )


async def report_loop(
    monitor: WashVolumeMonitor,
    interval_seconds: int,
    logger: Optional[LogFn] = None,
) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        emit_log(monitor.render_report(), logger)


async def runtime_guard(runtime_seconds: int, stop_event: asyncio.Event) -> None:
    if runtime_seconds <= 0:
        return
    await asyncio.sleep(runtime_seconds)
    stop_event.set()


async def stream_loop(
    symbol: str,
    monitor: WashVolumeMonitor,
    stop_event: asyncio.Event,
    logger: Optional[LogFn] = None,
) -> None:
    streams = [
        f"{symbol.lower()}@aggTrade",
        f"{symbol.lower()}@bookTicker",
        f"{symbol.lower()}@depth@100ms",
        f"{symbol.lower()}@kline_1m",
    ]
    url = WS_BASE + "/".join(streams)
    reconnect_delay = 2.0

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=1,
            ) as websocket:
                monitor.reset_depth_book()
                snapshot = await asyncio.to_thread(load_order_book_snapshot, symbol, 1000, logger)
                monitor.bootstrap_depth_book(snapshot)
                emit_log(
                    f"[{format_clock()}] depth_snapshot=ready lastUpdateId={snapshot['lastUpdateId']}",
                    logger,
                )
                emit_log(f"[{format_clock()}] connected={url}", logger)
                reconnect_delay = 2.0
                while not stop_event.is_set():
                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    payload = json.loads(message)
                    stream_name = payload.get("stream", "")
                    data = payload.get("data", {})
                    if stream_name.endswith("@aggTrade"):
                        monitor.on_trade(data)
                    elif stream_name.endswith("@bookTicker"):
                        monitor.on_book(data)
                    elif stream_name.endswith("@depth@100ms"):
                        try:
                            monitor.on_depth(data)
                        except OrderBookSyncError as exc:
                            emit_log(f"[{format_clock()}] warning={exc}; resyncing depth book", logger)
                            break
                    elif stream_name.endswith("@kline_1m"):
                        monitor.on_closed_kline(data)
        except asyncio.TimeoutError:
            emit_log(f"[{format_clock()}] warning=websocket idle timeout, reconnecting", logger)
        except (OSError, urllib.error.URLError, urllib.error.HTTPError, websockets.WebSocketException, json.JSONDecodeError) as exc:
            emit_log(f"[{format_clock()}] warning={exc.__class__.__name__}: {exc}", logger)

        if stop_event.is_set():
            break
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 1.5, 20.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monitor Binance spot taker flow and flag persistent two-sided wash-like volume."
    )
    parser.add_argument("--symbol", default="NIGHTUSDT", help="Spot symbol, for example NIGHTUSDT")
    parser.add_argument(
        "--windows",
        nargs="+",
        type=int,
        default=[300, 600],
        help="Rolling windows in seconds. Default: 300 600",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=10,
        help="Print report every N seconds. Default: 10",
    )
    parser.add_argument(
        "--baseline-minutes",
        type=int,
        default=120,
        help="Bootstrap baseline from the last N closed 1m klines. Max 1000. Default: 120",
    )
    parser.add_argument(
        "--runtime-seconds",
        type=int,
        default=0,
        help="Stop automatically after N seconds. Default: 0 means run forever",
    )
    parser.add_argument("--active-volume-ratio", type=float, default=1.40)
    parser.add_argument("--active-trade-ratio", type=float, default=1.15)
    parser.add_argument("--active-balance", type=float, default=0.68)
    parser.add_argument("--active-switch-rate", type=float, default=0.30)
    parser.add_argument("--suspicious-volume-ratio", type=float, default=2.20)
    parser.add_argument("--suspicious-trade-ratio", type=float, default=1.60)
    parser.add_argument("--suspicious-balance", type=float, default=0.82)
    parser.add_argument("--suspicious-switch-rate", type=float, default=0.45)
    parser.add_argument(
        "--suspicious-max-return",
        type=float,
        default=0.0040,
        help="Max allowed absolute price return inside the window. Default: 0.0040 = 0.40%%",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    if any(window <= 0 for window in args.windows):
        raise RuntimeError("all windows must be positive integers")
    if args.report_interval <= 0:
        raise RuntimeError("--report-interval must be > 0")

    monitor = create_monitor(
        symbol=args.symbol,
        windows=args.windows,
        thresholds=build_thresholds_from_args(args),
        baseline_minutes=args.baseline_minutes,
    )
    emit_log(monitor.render_report())

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signame):
            try:
                loop.add_signal_handler(getattr(signal, signame), stop_event.set)
            except NotImplementedError:
                pass

    tasks = [
        asyncio.create_task(stream_loop(args.symbol.upper(), monitor, stop_event)),
        asyncio.create_task(report_loop(monitor, args.report_interval)),
    ]
    if args.runtime_seconds > 0:
        tasks.append(asyncio.create_task(runtime_guard(args.runtime_seconds, stop_event)))

    await stop_event.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    emit_log(f"[{format_clock()}] stopped")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130
    except urllib.error.HTTPError as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        print(f"Network error: {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
