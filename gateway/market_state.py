from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .config import GatewayConfig


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _rounded(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(value, digits)


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _event_monotonic(timestamp_epoch: float) -> float:
    """Translate an exchange timestamp onto the local monotonic clock."""
    return time.monotonic() - max(0.0, time.time() - timestamp_epoch)


def _event_latency_ms(timestamp_epoch: float) -> float | None:
    """Return useful live latency without exposing placeholder/history ages."""
    age_ms = (time.time() - timestamp_epoch) * 1_000
    if age_ms < -5_000 or age_ms > 300_000:
        return None
    return max(0.0, age_ms)


@dataclass(slots=True)
class ProviderState:
    connected: bool = False
    last_update: str | None = None
    last_update_monotonic: float | None = None
    latency_ms: float | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "lastUpdate": self.last_update,
            "latencyMs": _rounded(self.latency_ms, 1),
            "error": self.error,
        }


@dataclass(slots=True)
class StreamState:
    state: str = "waiting"
    last_update: str | None = None
    last_update_monotonic: float | None = None
    error: str | None = None
    source: str | None = None

    def as_dict(
        self,
        *,
        now_monotonic: float | None = None,
        stale_after_ms: int | None = None,
    ) -> dict[str, Any]:
        effective_state = self.state
        if (
            effective_state == "live"
            and now_monotonic is not None
            and stale_after_ms is not None
            and self.last_update_monotonic is not None
            and (now_monotonic - self.last_update_monotonic) * 1_000 > stale_after_ms
        ):
            effective_state = "stale"
        return {
            "state": effective_state,
            "lastUpdate": self.last_update,
            "error": self.error,
            "source": self.source,
        }


class MarketState:
    """Single event-loop-owned state store for all real providers.

    Every number sent to the UI is either a provider value or a calculation made
    from provider values. Missing inputs remain ``None`` and render as an em dash.
    """

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.started_at = _iso_now()
        self.lock = asyncio.Lock()
        self.providers = {
            "topstepMarket": ProviderState(),
            "databento": ProviderState(),
            "alpaca": ProviderState(),
            "topstep": ProviderState(),
        }
        self.market_streams = {
            "bars": StreamState(),
            "quotes": StreamState(),
            "trades": StreamState(),
            "depth": StreamState(),
        }
        self.display_symbol: str | None = "NQ"
        self.market_source: str | None = None
        self.price: float | None = None
        self.bid: float | None = None
        self.ask: float | None = None
        self.market_timestamp: str | None = None
        self.market_monotonic: float | None = None
        self.bars: deque[dict[str, Any]] = deque(maxlen=1_200)
        # Minute history is kept separately from the one-second execution feed.
        # This gives Koda enough real structure for 15m, 30m, 1h, and 4h
        # context without bloating or slowing the live one-second calculations.
        self.context_bars: deque[dict[str, Any]] = deque(maxlen=12_000)
        self.trades: deque[dict[str, Any]] = deque(maxlen=50_000)
        self.depth: list[dict[str, float]] = []
        self.depth_bids: dict[float, float] = {}
        self.depth_asks: dict[float, float] = {}
        self.equities: dict[str, dict[str, Any] | None] = {
            symbol: None for symbol in config.alpaca_symbols
        }
        self.equity_opens: dict[str, float] = {}
        self.treasury_yields: dict[str, Any] = {
            "status": "disabled" if not config.treasury_yields_enabled else "waiting",
            "source": "U.S. Treasury daily par yield curve",
            "asOf": None,
            "twoYear": None,
            "tenYear": None,
            "curveSpreadBps": None,
            "twoYearChangeBps": None,
            "tenYearChangeBps": None,
            "fetchedAt": None,
            "error": None,
        }
        self.cftc_positioning: dict[str, Any] = {
            "status": "disabled" if not config.cftc_positioning_enabled else "waiting",
            "source": "CFTC Traders in Financial Futures - Futures Only",
            "asOf": None,
            "market": "NASDAQ-100 STOCK INDEX (MINI)",
            "openInterest": None,
            "assetManagerLong": None,
            "assetManagerShort": None,
            "assetManagerNet": None,
            "assetManagerNetChange": None,
            "leveragedLong": None,
            "leveragedShort": None,
            "leveragedNet": None,
            "leveragedNetChange": None,
            "fetchedAt": None,
            "error": None,
        }
        self.topstep_account: dict[str, Any] | None = None
        self.topstep_positions: list[dict[str, Any]] = []
        self.topstep_orders: list[dict[str, Any]] = []
        self.last_velocity: float | None = None
        self.last_velocity_at: float | None = None
        self.last_gateway_error: str | None = None

    async def set_provider(
        self,
        provider: str,
        *,
        connected: bool | None = None,
        error: str | None = None,
        latency_ms: float | None = None,
        touched: bool = False,
    ) -> None:
        async with self.lock:
            state = self.providers[provider]
            if connected is not None:
                state.connected = connected
            state.error = error
            if latency_ms is not None:
                state.latency_ms = latency_ms
            if touched:
                state.last_update = _iso_now()
                state.last_update_monotonic = time.monotonic()

    async def set_gateway_error(self, message: str | None) -> None:
        async with self.lock:
            self.last_gateway_error = message

    async def set_treasury_yields(
        self,
        *,
        as_of: str,
        two_year: float,
        ten_year: float,
        two_year_change_bps: float | None,
        ten_year_change_bps: float | None,
    ) -> None:
        async with self.lock:
            self.treasury_yields = {
                "status": "daily",
                "source": "U.S. Treasury daily par yield curve",
                "asOf": as_of,
                "twoYear": _rounded(two_year, 3),
                "tenYear": _rounded(ten_year, 3),
                "curveSpreadBps": _rounded((ten_year - two_year) * 100, 1),
                "twoYearChangeBps": _rounded(two_year_change_bps, 1),
                "tenYearChangeBps": _rounded(ten_year_change_bps, 1),
                "fetchedAt": _iso_now(),
                "error": None,
            }

    async def set_treasury_error(self, message: str) -> None:
        async with self.lock:
            self.treasury_yields = {**self.treasury_yields, "status": "error", "error": message}

    async def set_cftc_positioning(self, payload: dict[str, Any]) -> None:
        async with self.lock:
            self.cftc_positioning = {
                **self.cftc_positioning,
                **payload,
                "status": "weekly",
                "source": "CFTC Traders in Financial Futures - Futures Only",
                "fetchedAt": _iso_now(),
                "error": None,
            }

    async def set_cftc_error(self, message: str) -> None:
        async with self.lock:
            self.cftc_positioning = {**self.cftc_positioning, "status": "error", "error": message}

    def _mark_stream_live(
        self,
        stream: str,
        source: str,
        event_monotonic: float | None = None,
    ) -> None:
        item = self.market_streams[stream]
        observed_monotonic = event_monotonic if event_monotonic is not None else time.monotonic()
        # A historical catch-up event must not take ownership from a newer
        # event supplied by the other provider in hybrid mode.
        if (
            item.last_update_monotonic is not None
            and observed_monotonic < item.last_update_monotonic
        ):
            return
        item.state = "live"
        item.error = None
        item.source = source
        item.last_update = _iso_now()
        item.last_update_monotonic = observed_monotonic

    def _stream_is_fresh(self, stream: str, now_monotonic: float) -> bool:
        item = self.market_streams[stream]
        if item.state != "live" or item.last_update_monotonic is None:
            return False
        return (now_monotonic - item.last_update_monotonic) * 1_000 <= self.config.stale_after_ms

    async def set_market_stream(
        self,
        stream: str,
        *,
        state: str,
        error: str | None = None,
        source: str = "topstepMarket",
    ) -> None:
        """Record an effective market stream without downgrading a live alternate source."""
        if stream not in self.market_streams:
            return
        async with self.lock:
            item = self.market_streams[stream]
            if item.state == "live" and item.source != source and state != "live":
                return
            item.state = state
            item.error = error
            item.source = source
            if state == "live":
                item.last_update = _iso_now()
                item.last_update_monotonic = time.monotonic()

    async def apply_symbol_mapping(self, symbol: str) -> None:
        async with self.lock:
            self.display_symbol = symbol

    async def apply_bar(
        self,
        *,
        timestamp: str,
        timestamp_epoch: float,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        provider_name: str = "databento",
    ) -> None:
        if not all(math.isfinite(value) for value in (open_price, high, low, close, volume)):
            return
        async with self.lock:
            observed_monotonic = _event_monotonic(timestamp_epoch)
            bar = {
                "ts": timestamp,
                "epoch": timestamp_epoch,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            if self.bars and self.bars[-1]["ts"] == timestamp:
                self.bars[-1] = bar
            elif not self.bars or timestamp_epoch >= self.bars[-1]["epoch"]:
                self.bars.append(bar)
            else:
                merged = {item["ts"]: item for item in self.bars}
                merged[timestamp] = bar
                ordered = sorted(merged.values(), key=lambda item: item["epoch"])[-self.bars.maxlen :]
                self.bars = deque(ordered, maxlen=self.bars.maxlen)

            quote_stream = self.market_streams["quotes"]
            quote_is_fresh = (
                quote_stream.state == "live"
                and quote_stream.last_update_monotonic is not None
                and (time.monotonic() - quote_stream.last_update_monotonic) * 1_000
                <= self.config.stale_after_ms
            )
            if not quote_is_fresh:
                self.price = close
                self.market_timestamp = timestamp
                self.market_monotonic = observed_monotonic
                self.market_source = provider_name
            provider = self.providers[provider_name]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = observed_monotonic
            provider.latency_ms = _event_latency_ms(timestamp_epoch)
            self._mark_stream_live("bars", provider_name, observed_monotonic)

    async def apply_context_bar(
        self,
        *,
        timestamp: str,
        timestamp_epoch: float,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
    ) -> None:
        """Merge a real historical one-minute bar for multi-timeframe analysis."""
        if not all(math.isfinite(value) for value in (open_price, high, low, close, volume)):
            return
        async with self.lock:
            bar = {
                "ts": timestamp,
                "epoch": timestamp_epoch,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            if self.context_bars and self.context_bars[-1]["ts"] == timestamp:
                self.context_bars[-1] = bar
            elif not self.context_bars or timestamp_epoch >= self.context_bars[-1]["epoch"]:
                self.context_bars.append(bar)
            else:
                merged = {item["ts"]: item for item in self.context_bars}
                merged[timestamp] = bar
                ordered = sorted(merged.values(), key=lambda item: item["epoch"])[-self.context_bars.maxlen :]
                self.context_bars = deque(ordered, maxlen=self.context_bars.maxlen)

    async def apply_trade(
        self,
        *,
        timestamp: str,
        timestamp_epoch: float,
        price: float,
        size: float,
        aggressor: str,
        provider_name: str = "databento",
        build_one_second_bar: bool = False,
    ) -> None:
        if not math.isfinite(price) or not math.isfinite(size):
            return
        side = aggressor.upper()
        signed_size = size if side in {"B", "BID", "BUY"} else -size if side in {"A", "ASK", "SELL"} else 0.0
        async with self.lock:
            observed_monotonic = _event_monotonic(timestamp_epoch)
            self.trades.append(
                {
                    "ts": timestamp,
                    "epoch": timestamp_epoch,
                    "price": price,
                    "size": size,
                    "signed_size": signed_size,
                }
            )
            if build_one_second_bar:
                second_epoch = math.floor(timestamp_epoch)
                second_timestamp = datetime.fromtimestamp(second_epoch, UTC).isoformat()
                if self.bars and int(float(self.bars[-1]["epoch"])) == second_epoch:
                    current = dict(self.bars[-1])
                    current["high"] = max(float(current["high"]), price)
                    current["low"] = min(float(current["low"]), price)
                    current["close"] = price
                    current["volume"] = float(current["volume"]) + max(0.0, size)
                    self.bars[-1] = current
                elif not self.bars or second_epoch > int(float(self.bars[-1]["epoch"])):
                    self.bars.append(
                        {
                            "ts": second_timestamp,
                            "epoch": float(second_epoch),
                            "open": price,
                            "high": price,
                            "low": price,
                            "close": price,
                            "volume": max(0.0, size),
                        }
                    )
            # Topstep remains the authoritative last-price source in hybrid mode.
            # Databento can safely seed the price only while Topstep has not produced one.
            if provider_name == "topstepMarket" or self.price is None:
                self.price = price
                self.market_timestamp = timestamp
                self.market_monotonic = observed_monotonic
                self.market_source = provider_name
            provider = self.providers[provider_name]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = observed_monotonic
            provider.latency_ms = _event_latency_ms(timestamp_epoch)
            self._mark_stream_live("trades", provider_name, observed_monotonic)

    async def apply_depth(
        self,
        *,
        timestamp: str,
        timestamp_epoch: float,
        levels: list[dict[str, float]],
        provider_name: str = "databento",
    ) -> None:
        clean_levels = [
            level
            for level in levels
            if level.get("bidPrice", 0) > 0
            and level.get("askPrice", 0) > 0
            and level.get("bidSize", 0) >= 0
            and level.get("askSize", 0) >= 0
        ]
        if not clean_levels:
            return
        async with self.lock:
            observed_monotonic = _event_monotonic(timestamp_epoch)
            self.depth = clean_levels[:10]
            self.bid = clean_levels[0]["bidPrice"]
            self.ask = clean_levels[0]["askPrice"]
            provider = self.providers[provider_name]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = observed_monotonic
            provider.latency_ms = _event_latency_ms(timestamp_epoch)
            self._mark_stream_live("depth", provider_name, observed_monotonic)

    async def apply_quote(
        self,
        *,
        timestamp: str,
        timestamp_epoch: float,
        last_price: float | None,
        bid: float | None,
        ask: float | None,
        provider_name: str = "topstepMarket",
    ) -> None:
        async with self.lock:
            observed_monotonic = _event_monotonic(timestamp_epoch)
            if last_price is not None and math.isfinite(last_price):
                self.price = last_price
            if bid is not None and math.isfinite(bid):
                self.bid = bid
            if ask is not None and math.isfinite(ask):
                self.ask = ask
            self.market_timestamp = timestamp
            self.market_monotonic = observed_monotonic
            self.market_source = provider_name
            provider = self.providers[provider_name]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = observed_monotonic
            provider.latency_ms = _event_latency_ms(timestamp_epoch)
            self._mark_stream_live("quotes", provider_name, observed_monotonic)

    async def apply_depth_update(
        self,
        *,
        timestamp: str,
        timestamp_epoch: float,
        depth_type: int,
        price: float | None,
        current_volume: float | None,
        provider_name: str = "topstepMarket",
    ) -> None:
        async with self.lock:
            observed_monotonic = _event_monotonic(timestamp_epoch)
            if depth_type == 6:
                self.depth_bids.clear()
                self.depth_asks.clear()
                self.depth = []
            elif price is not None and price > 0 and current_volume is not None:
                book = self.depth_asks if depth_type in {1, 3, 10} else self.depth_bids if depth_type in {2, 4, 9} else None
                if book is not None:
                    if current_volume <= 0:
                        book.pop(price, None)
                    else:
                        book[price] = current_volume

            bid_rows = sorted(self.depth_bids.items(), key=lambda row: row[0], reverse=True)[:10]
            ask_rows = sorted(self.depth_asks.items(), key=lambda row: row[0])[:10]
            if bid_rows:
                self.bid = bid_rows[0][0]
            if ask_rows:
                self.ask = ask_rows[0][0]
            row_count = max(len(bid_rows), len(ask_rows))
            self.depth = [
                {
                    "bidPrice": bid_rows[index][0] if index < len(bid_rows) else 0.0,
                    "bidSize": bid_rows[index][1] if index < len(bid_rows) else 0.0,
                    "askPrice": ask_rows[index][0] if index < len(ask_rows) else 0.0,
                    "askSize": ask_rows[index][1] if index < len(ask_rows) else 0.0,
                }
                for index in range(row_count)
            ]
            self.market_timestamp = timestamp
            self.market_monotonic = observed_monotonic
            self.market_source = provider_name
            provider = self.providers[provider_name]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = observed_monotonic
            provider.latency_ms = _event_latency_ms(timestamp_epoch)
            self._mark_stream_live("depth", provider_name, observed_monotonic)

    async def apply_equity_event(self, event: dict[str, Any]) -> None:
        symbol = str(event.get("S", "")).upper()
        if symbol not in self.equities:
            return
        event_type = str(event.get("T", ""))
        timestamp = str(event.get("t") or _iso_now())
        async with self.lock:
            current = dict(self.equities[symbol] or {})
            reference_open = _finite(event.get("referenceOpen"))
            if reference_open is not None and reference_open > 0:
                self.equity_opens[symbol] = reference_open
            if event_type == "q":
                current["bid"] = _finite(event.get("bp"))
                current["ask"] = _finite(event.get("ap"))
                bid, ask = current.get("bid"), current.get("ask")
                if bid is not None and ask is not None:
                    current["price"] = (bid + ask) / 2
            elif event_type == "t":
                current["price"] = _finite(event.get("p"))
            elif event_type in {"b", "u", "d"}:
                close = _finite(event.get("c"))
                open_price = _finite(event.get("o"))
                if close is not None:
                    current["price"] = close
                if open_price is not None and symbol not in self.equity_opens:
                    self.equity_opens[symbol] = open_price

            price = _finite(current.get("price"))
            reference = self.equity_opens.get(symbol)
            change_pct = ((price / reference) - 1) * 100 if price and reference else None
            current.update(
                {
                    "symbol": symbol,
                    "price": price,
                    "bid": _finite(current.get("bid")),
                    "ask": _finite(current.get("ask")),
                    "changePct": change_pct,
                    "timestamp": timestamp,
                }
            )
            self.equities[symbol] = current
            provider = self.providers["alpaca"]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = time.monotonic()

    async def apply_topstep_snapshot(
        self,
        *,
        account: dict[str, Any] | None,
        positions: list[dict[str, Any]],
        orders: list[dict[str, Any]],
        latency_ms: float,
    ) -> None:
        clean_positions: list[dict[str, Any]] = []
        for position in positions:
            position_type = int(position.get("type", 0) or 0)
            clean_positions.append(
                {
                    "id": int(position.get("id", 0) or 0),
                    "contractId": str(position.get("contractId", "")),
                    "side": "LONG" if position_type == 1 else "SHORT",
                    "size": int(position.get("size", 0) or 0),
                    "averagePrice": float(position.get("averagePrice", 0) or 0),
                }
            )

        order_types = {1: "LIMIT", 2: "MARKET", 4: "STOP", 5: "TRAILING STOP", 6: "JOIN BID", 7: "JOIN ASK"}
        order_statuses = {0: "NONE", 1: "OPEN", 2: "FILLED", 3: "CANCELLED", 4: "EXPIRED", 5: "REJECTED", 6: "PENDING"}
        clean_orders: list[dict[str, Any]] = []
        for order in orders:
            clean_orders.append(
                {
                    "id": int(order.get("id", 0) or 0),
                    "contractId": str(order.get("contractId", "")),
                    "side": "BUY" if int(order.get("side", 0) or 0) == 0 else "SELL",
                    "type": order_types.get(int(order.get("type", 0) or 0), "UNKNOWN"),
                    "size": int(order.get("size", 0) or 0),
                    "limitPrice": _finite(order.get("limitPrice")),
                    "stopPrice": _finite(order.get("stopPrice")),
                    "status": order_statuses.get(int(order.get("status", 0) or 0), "UNKNOWN"),
                }
            )

        async with self.lock:
            self.topstep_account = (
                {
                    "id": int(account.get("id", 0) or 0),
                    "name": str(account.get("name", "Topstep account")),
                    "canTrade": bool(account.get("canTrade", False)),
                }
                if account
                else None
            )
            self.topstep_positions = clean_positions
            self.topstep_orders = clean_orders
            provider = self.providers["topstep"]
            provider.connected = True
            provider.error = None
            provider.last_update = _iso_now()
            provider.last_update_monotonic = time.monotonic()
            provider.latency_ms = latency_ms

    def _price_at_or_before(self, epoch: float, bars: list[dict[str, Any]]) -> float | None:
        for bar in reversed(bars):
            if bar["epoch"] <= epoch:
                return float(bar["close"])
        return None

    def _round_to_tick(self, value: float | None) -> float | None:
        if value is None:
            return None
        tick = 0.25
        return round(round(value / tick) * tick, 2)

    def _calculate(self, now_monotonic: float) -> dict[str, Any]:
        bars = list(self.bars)
        price = self.price
        stale_age_ms = (
            (now_monotonic - self.market_monotonic) * 1_000
            if self.market_monotonic is not None
            else None
        )
        stale = stale_age_ms is None or stale_age_ms > self.config.stale_after_ms

        if price is None or len(bars) < 3:
            return {
                "stale": stale,
                "staleAgeMs": stale_age_ms,
                "ready": False,
                "metrics": self._empty_metrics(),
                "levels": self._empty_levels(),
            }

        ranges = [max(0.0, float(bar["high"]) - float(bar["low"])) for bar in bars[-120:]]
        atr = sum(ranges) / len(ranges) if ranges else None
        session_volume = sum(max(0.0, float(bar["volume"])) for bar in bars)
        vwap_numerator = sum(
            ((float(bar["high"]) + float(bar["low"]) + float(bar["close"])) / 3) * max(0.0, float(bar["volume"]))
            for bar in bars
        )
        vwap = vwap_numerator / session_volume if session_volume > 0 else None

        latest_epoch = float(bars[-1]["epoch"])
        price_1s = self._price_at_or_before(latest_epoch - 1, bars)
        price_5s = self._price_at_or_before(latest_epoch - 5, bars)
        velocity = price - price_1s if price_1s is not None else None
        momentum_change = price - price_5s if price_5s is not None else None
        volatility_scale = max((atr or 0) * 4, 1.0)
        momentum_signal = math.tanh(momentum_change / volatility_scale) if momentum_change is not None else 0.0

        recent_trade_cutoff = time.time() - 10
        recent_trades = [trade for trade in self.trades if trade["epoch"] >= recent_trade_cutoff]
        cumulative_delta = sum(float(trade["signed_size"]) for trade in recent_trades)
        total_trade_size = sum(float(trade["size"]) for trade in recent_trades)
        delta_signal = cumulative_delta / total_trade_size if total_trade_size > 0 else None

        total_bid = sum(float(level["bidSize"]) for level in self.depth)
        total_ask = sum(float(level["askSize"]) for level in self.depth)
        book_total = total_bid + total_ask
        book_imbalance = (total_bid - total_ask) / book_total if book_total > 0 else None

        equity_values = [
            float(quote["changePct"])
            for quote in self.equities.values()
            if quote and quote.get("changePct") is not None
        ]
        equity_signal = math.tanh((sum(equity_values) / len(equity_values)) / 0.5) if equity_values else 0.0

        composite = _clamp(
            momentum_signal * 0.42 + (delta_signal or 0.0) * 0.28 + (book_imbalance or 0.0) * 0.20 + equity_signal * 0.10,
            -1,
            1,
        )
        bias = "BULLISH" if composite >= 0.12 else "BEARISH" if composite <= -0.12 else "NEUTRAL"
        momentum_strength = abs(momentum_signal) * 100
        flow_strength = abs(delta_signal) * 100 if delta_signal is not None else None
        liquidity_strength = abs(book_imbalance) * 100 if book_imbalance is not None else None
        bars_fresh = self._stream_is_fresh("bars", now_monotonic)
        trades_fresh = self._stream_is_fresh("trades", now_monotonic)
        depth_fresh = self._stream_is_fresh("depth", now_monotonic)
        coverage_inputs = (
            (len(bars) >= 10 and bars_fresh, bool(equity_values))
            if self.config.suggestion_only
            else (
                len(bars) >= 10 and bars_fresh,
                total_trade_size > 0 and trades_fresh,
                bool(self.depth) and depth_fresh,
                bool(equity_values),
            )
        )
        coverage = sum(1 for available in coverage_inputs if available) / len(coverage_inputs)
        confidence = _clamp(
            abs(composite) * 65 + ((momentum_strength + (flow_strength or 0) + (liquidity_strength or 0)) / 3) * 0.25 + coverage * 10,
            0,
            100,
        )
        required_inputs = (
            len(bars) >= 20 and bars_fresh
            if self.config.suggestion_only
            else (
                len(bars) >= 10
                and total_trade_size > 0
                and bool(self.depth)
                and bars_fresh
                and trades_fresh
                and depth_fresh
            )
        )

        acceleration: float | None = None
        if velocity is not None and self.last_velocity is not None and self.last_velocity_at is not None:
            elapsed = max(0.001, now_monotonic - self.last_velocity_at)
            acceleration = (velocity - self.last_velocity) / elapsed
        if velocity is not None:
            self.last_velocity = velocity
            self.last_velocity_at = now_monotonic

        # A one-second ATR is useful for a stop buffer, but it is far too small
        # to describe the actual opportunity in an NQ scalp.  Build the target
        # model from the real rolling market structure as well: the last three
        # minutes of provider bars show the range the market has actually been
        # capable of covering.  We only take a conservative fraction of that
        # range and cap it so a single abnormal print cannot produce a fantasy
        # target.  The UI engines apply their own setup-specific target floors,
        # so this remains a neutral opportunity estimate rather than an entry gate.
        structure_bars = bars[-180:]
        structure_high = max(float(bar["high"]) for bar in structure_bars)
        structure_low = min(float(bar["low"]) for bar in structure_bars)
        structure_range = max(0.0, structure_high - structure_low)
        micro_burst = max((atr or 0) * 6, 1.0) if atr is not None else None
        expected_burst = (
            min(40.0, max(micro_burst or 0.0, structure_range * 0.4))
            if micro_burst is not None
            else None
        )
        direction = 1 if bias == "BULLISH" else -1 if bias == "BEARISH" else 0
        trigger = self._round_to_tick(price + direction * 0.25) if direction else None
        tp1 = self._round_to_tick(price + direction * expected_burst * 0.55) if direction and expected_burst else None
        tp2 = self._round_to_tick(price + direction * expected_burst) if direction and expected_burst else None
        stretch = self._round_to_tick(price + direction * expected_burst * 1.5) if direction and expected_burst else None
        invalidation = self._round_to_tick(price - direction * expected_burst * 0.32) if direction and expected_burst else None

        buy_liquidity = None
        sell_liquidity = None
        if self.depth and depth_fresh:
            buy_liquidity = max(self.depth, key=lambda level: level["bidSize"])["bidPrice"]
            sell_liquidity = max(self.depth, key=lambda level: level["askSize"])["askPrice"]

        eta_mid = None
        if expected_burst and velocity and abs(velocity) > 0.01:
            eta_mid = _clamp(expected_burst / abs(velocity), 8, 180)

        metrics = {
            "bias": bias,
            "confidence": round(confidence) if required_inputs else None,
            "momentum": round(momentum_strength),
            "orderFlow": round(flow_strength) if flow_strength is not None and trades_fresh else None,
            "liquidity": round(liquidity_strength) if liquidity_strength is not None and depth_fresh else None,
            "momentumSignal": _rounded(momentum_signal, 4),
            "deltaSignal": _rounded(delta_signal, 4),
            "bookImbalance": _rounded(book_imbalance, 4),
            "velocity": _rounded(velocity, 3),
            "acceleration": _rounded(acceleration, 3),
            "cumulativeDelta": _rounded(cumulative_delta, 0),
            "atr": _rounded(atr, 3),
            "vwap": _rounded(vwap, 2),
            "sessionVolume": _rounded(session_volume, 0),
        }
        levels = {
            "buyLiquidity": self._round_to_tick(buy_liquidity),
            "sellLiquidity": self._round_to_tick(sell_liquidity),
            "trigger": trigger if required_inputs else None,
            "tp1": tp1 if required_inputs else None,
            "tp2": tp2 if required_inputs else None,
            "stretch": stretch if required_inputs else None,
            "invalidation": invalidation if required_inputs else None,
            "expectedBurst": _rounded(expected_burst, 2) if required_inputs else None,
            "estimatedSecondsMin": round(eta_mid * 0.7) if required_inputs and eta_mid is not None else None,
            "estimatedSecondsMax": round(eta_mid * 1.3) if required_inputs and eta_mid is not None else None,
        }
        return {
            "stale": stale,
            "staleAgeMs": stale_age_ms,
            "ready": bool(not stale and required_inputs and bias != "NEUTRAL"),
            "metrics": metrics,
            "levels": levels,
        }

    @staticmethod
    def _empty_metrics() -> dict[str, Any]:
        return {
            "bias": "NEUTRAL",
            "confidence": None,
            "momentum": None,
            "orderFlow": None,
            "liquidity": None,
            "momentumSignal": None,
            "deltaSignal": None,
            "bookImbalance": None,
            "velocity": None,
            "acceleration": None,
            "cumulativeDelta": None,
            "atr": None,
            "vwap": None,
            "sessionVolume": None,
        }

    @staticmethod
    def _empty_levels() -> dict[str, Any]:
        return {
            "buyLiquidity": None,
            "sellLiquidity": None,
            "trigger": None,
            "tp1": None,
            "tp2": None,
            "stretch": None,
            "invalidation": None,
            "expectedBurst": None,
            "estimatedSecondsMin": None,
            "estimatedSecondsMax": None,
        }

    @staticmethod
    def _aggregate_context(
        bars: list[dict[str, Any]],
        minutes: int,
        *,
        limit: int = 32,
    ) -> list[dict[str, Any]]:
        """Aggregate real one-minute history into compact UI timeframe bars."""
        seconds = minutes * 60
        grouped: dict[int, dict[str, Any]] = {}
        for bar in bars:
            epoch = float(bar["epoch"])
            bucket = int(epoch // seconds) * seconds
            existing = grouped.get(bucket)
            if existing is None:
                grouped[bucket] = {
                    "ts": datetime.fromtimestamp(bucket, UTC).isoformat(),
                    "epoch": bucket,
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "volume": max(0.0, float(bar["volume"])),
                }
                continue
            existing["high"] = max(float(existing["high"]), float(bar["high"]))
            existing["low"] = min(float(existing["low"]), float(bar["low"]))
            existing["close"] = float(bar["close"])
            existing["volume"] = float(existing["volume"]) + max(0.0, float(bar["volume"]))

        now_epoch = time.time()
        result = []
        for item in sorted(grouped.values(), key=lambda row: float(row["epoch"]))[-limit:]:
            result.append({
                "ts": item["ts"],
                "open": _rounded(float(item["open"]), 2),
                "high": _rounded(float(item["high"]), 2),
                "low": _rounded(float(item["low"]), 2),
                "close": _rounded(float(item["close"]), 2),
                "volume": _rounded(float(item["volume"]), 0),
                "complete": now_epoch >= float(item["epoch"]) + seconds,
            })
        return result

    async def snapshot(self) -> dict[str, Any]:
        async with self.lock:
            now_monotonic = time.monotonic()
            calculated = self._calculate(now_monotonic)
            stale = bool(calculated["stale"])
            topstep_market = self.providers["topstepMarket"]
            stream_snapshots = {
                name: stream.as_dict(
                    now_monotonic=now_monotonic,
                    stale_after_ms=self.config.stale_after_ms,
                )
                for name, stream in self.market_streams.items()
            }
            if self.last_gateway_error:
                status = "error"
                message = self.last_gateway_error
            elif not self.config.suggestion_only and topstep_market.error and self.price is None:
                status = "error"
                message = topstep_market.error
            elif self.price is None:
                status = "connecting"
                message = "Waiting for the first hosted Databento NQ update." if self.config.suggestion_only else "Waiting for the first Topstep NQ market update."
            elif stale:
                status = "stale"
                message = "The hosted NQ feed is stale. Suggestions are locked until fresh market data returns." if self.config.suggestion_only else "The Topstep NQ feed is stale. Signals are locked until fresh market data returns."
            else:
                status = "connected"
                required_names = ({
                    "databento": "Databento NQ",
                    "alpaca": "Alpaca context",
                } if self.config.suggestion_only else {
                    "topstepMarket": "Topstep NQ",
                    "alpaca": "Alpaca context",
                    "topstep": "Topstep account",
                })
                missing = [label for name, label in required_names.items() if not self.providers[name].connected]
                incomplete_streams = [
                    name
                    for name, stream in stream_snapshots.items()
                    if stream["state"] != "live"
                ]
                if incomplete_streams and not self.config.suggestion_only:
                    details = ", ".join(
                        f"{name.upper()} {stream_snapshots[name]['state'].upper()}"
                        for name in incomplete_streams
                    )
                    message = f"Hybrid NQ is partial: {details}. Koda unlocks only after real events arrive."
                else:
                    sources = sorted(
                        {
                            str(stream["source"])
                            for stream in stream_snapshots.values()
                            if stream["source"]
                        }
                    )
                    source_label = " + ".join(
                        "Topstep" if source == "topstepMarket" else "Databento" if source == "databento" else source
                        for source in sources
                    )
                    message = (
                        "Hosted Databento NQ suggestions are live. Order execution is permanently disabled."
                        if self.config.suggestion_only and not missing
                        else f"All required real NQ streams are live through {source_label}."
                        if not missing
                        else f"NQ streams are live. Still connecting: {', '.join(missing)}."
                    )

            bars = [
                {
                    "ts": bar["ts"],
                    "open": _rounded(float(bar["open"]), 2),
                    "high": _rounded(float(bar["high"]), 2),
                    "low": _rounded(float(bar["low"]), 2),
                    "close": _rounded(float(bar["close"]), 2),
                    "volume": _rounded(float(bar["volume"]), 0),
                }
                for bar in list(self.bars)[-120:]
            ]
            context_bars = list(self.context_bars)
            timeframes = {
                "5m": self._aggregate_context(context_bars, 5),
                "15m": self._aggregate_context(context_bars, 15),
                "30m": self._aggregate_context(context_bars, 30),
                "1h": self._aggregate_context(context_bars, 60),
                "4h": self._aggregate_context(context_bars, 240),
            }
            equities = {
                symbol: (
                    {
                        "symbol": symbol,
                        "price": _rounded(_finite(quote.get("price")), 2),
                        "bid": _rounded(_finite(quote.get("bid")), 2),
                        "ask": _rounded(_finite(quote.get("ask")), 2),
                        "changePct": _rounded(_finite(quote.get("changePct")), 3),
                        "timestamp": str(quote.get("timestamp") or _iso_now()),
                    }
                    if quote and quote.get("price") is not None
                    else None
                )
                for symbol, quote in self.equities.items()
            }
            book_rows = [
                {
                    "level": index + 1,
                    "bidPrice": _rounded(_finite(level.get("bidPrice")), 2)
                    if (_finite(level.get("bidPrice")) or 0) > 0
                    else None,
                    "bidSize": _rounded(max(0.0, _finite(level.get("bidSize")) or 0.0), 0),
                    "askPrice": _rounded(_finite(level.get("askPrice")), 2)
                    if (_finite(level.get("askPrice")) or 0) > 0
                    else None,
                    "askSize": _rounded(max(0.0, _finite(level.get("askSize")) or 0.0), 0),
                }
                for index, level in enumerate(self.depth[:10])
            ]
            total_bid_size = sum(float(level["bidSize"] or 0) for level in book_rows)
            total_ask_size = sum(float(level["askSize"] or 0) for level in book_rows)
            depth_total = total_bid_size + total_ask_size
            priced_bids = [level for level in book_rows if level["bidPrice"] is not None]
            priced_asks = [level for level in book_rows if level["askPrice"] is not None]
            largest_bid = max(priced_bids, key=lambda level: float(level["bidSize"] or 0), default=None)
            largest_ask = max(priced_asks, key=lambda level: float(level["askSize"] or 0), default=None)
            best_bid = priced_bids[0]["bidPrice"] if priced_bids else _rounded(self.bid, 2)
            best_ask = priced_asks[0]["askPrice"] if priced_asks else _rounded(self.ask, 2)
            spread = (
                float(best_ask) - float(best_bid)
                if best_bid is not None and best_ask is not None
                else None
            )
            tape_cutoff = time.time() - 60
            tape = [
                {
                    "ts": str(trade["ts"]),
                    "price": _rounded(float(trade["price"]), 2),
                    "size": _rounded(max(0.0, float(trade["size"])), 0),
                    "side": "BUY"
                    if float(trade["signed_size"]) > 0
                    else "SELL"
                    if float(trade["signed_size"]) < 0
                    else "UNKNOWN",
                }
                for trade in list(self.trades)
                if float(trade["epoch"]) >= tape_cutoff
            ][-80:]
            return {
                "type": "snapshot",
                "version": 1,
                "generatedAt": _iso_now(),
                "gatewayStartedAt": self.started_at,
                "status": status,
                "stale": stale,
                "staleAgeMs": _rounded(calculated["staleAgeMs"], 0),
                "ready": calculated["ready"],
                "message": message,
                "providers": {name: provider.as_dict() for name, provider in self.providers.items()},
                "marketStreams": stream_snapshots,
                "routing": {
                    "bars": stream_snapshots["bars"]["source"],
                    "quotes": stream_snapshots["quotes"]["source"],
                    "trades": stream_snapshots["trades"]["source"],
                    "depth": stream_snapshots["depth"]["source"],
                    "databentoEnabled": self.config.databento_enabled,
                },
                "instrument": {
                    "symbol": "NQ",
                    "displaySymbol": self.display_symbol,
                    "source": self.market_source,
                    "price": _rounded(self.price, 2),
                    "bid": _rounded(self.bid, 2),
                    "ask": _rounded(self.ask, 2),
                    "tickSize": 0.25,
                    "timestamp": self.market_timestamp,
                },
                "bars": bars,
                "timeframes": timeframes,
                "orderBook": {
                    "timestamp": stream_snapshots["depth"]["lastUpdate"],
                    "source": stream_snapshots["depth"]["source"],
                    "levels": book_rows,
                    "bestBid": best_bid,
                    "bestAsk": best_ask,
                    "spread": _rounded(spread, 2),
                    "totalBidSize": _rounded(total_bid_size, 0),
                    "totalAskSize": _rounded(total_ask_size, 0),
                    "imbalance": _rounded(
                        (total_bid_size - total_ask_size) / depth_total if depth_total > 0 else None,
                        4,
                    ),
                    "largestBid": (
                        {"price": largest_bid["bidPrice"], "size": largest_bid["bidSize"]}
                        if largest_bid
                        else None
                    ),
                    "largestAsk": (
                        {"price": largest_ask["askPrice"], "size": largest_ask["askSize"]}
                        if largest_ask
                        else None
                    ),
                },
                "tape": tape,
                "metrics": calculated["metrics"],
                "levels": calculated["levels"],
                "equities": equities,
                "macro": {
                    "treasury": dict(self.treasury_yields),
                    "cftc": dict(self.cftc_positioning),
                },
                "topstep": {
                    "account": self.topstep_account,
                    "positions": self.topstep_positions,
                    "orders": self.topstep_orders,
                },
            }
