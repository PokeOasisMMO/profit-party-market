from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import databento as db

from ..config import GatewayConfig
from ..market_state import MarketState


def _price(value: Any) -> float | None:
    if value is None:
        return None
    for method_name in ("as_double", "to_double", "as_float"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                return float(method())
            except (TypeError, ValueError):
                pass
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if abs(number) >= 9_000_000_000_000_000_000:
        return None
    if abs(number) > 10_000_000:
        number /= 1_000_000_000
    return number


def _timestamp(record: Any) -> tuple[str, float]:
    raw = getattr(record, "ts_event", None) or getattr(record, "ts_recv", None)
    try:
        nanoseconds = int(raw)
        epoch = nanoseconds / 1_000_000_000
        return datetime.fromtimestamp(epoch, UTC).isoformat(), epoch
    except (TypeError, ValueError, OSError, OverflowError):
        now = datetime.now(UTC)
        return now.isoformat(), now.timestamp()


def _enum_text(value: Any) -> str:
    raw = str(value or "").upper()
    if "." in raw:
        raw = raw.rsplit(".", 1)[-1]
    return raw


class DatabentoFeed:
    def __init__(self, config: GatewayConfig, state: MarketState) -> None:
        self.config = config
        self.state = state
        self.loop: asyncio.AbstractEventLoop | None = None
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=75_000)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.live: db.Live | None = None
        self.consumer_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not self.config.databento_api_key:
            await self.state.set_provider(
                "databento",
                connected=False,
                error="DATABENTO_API_KEY is missing from gateway/.env.",
            )
            return
        self.loop = asyncio.get_running_loop()
        self.consumer_task = asyncio.create_task(self._consume(), name="databento-consumer")
        self.thread = threading.Thread(target=self._run, name="databento-live", daemon=True)
        self.thread.start()

    async def stop(self) -> None:
        self.stop_event.set()
        if self.live is not None:
            try:
                self.live.stop()
            except Exception:
                pass
        if self.consumer_task:
            self.consumer_task.cancel()
            await asyncio.gather(self.consumer_task, return_exceptions=True)
        if self.thread and self.thread.is_alive():
            await asyncio.to_thread(self.thread.join, 3)

    def _enqueue(self, record: Any) -> None:
        if self.loop is None or self.stop_event.is_set():
            return

        def put() -> None:
            try:
                self.queue.put_nowait(record)
            except asyncio.QueueFull:
                asyncio.create_task(
                    self.state.set_provider(
                        "databento",
                        connected=False,
                        error="Databento consumer fell behind; signals locked while reconnecting.",
                    )
                )

        self.loop.call_soon_threadsafe(put)

    def _run(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                self.live = db.Live(
                    key=self.config.databento_api_key,
                    heartbeat_interval_s=5,
                    slow_reader_behavior="skip",
                )
                replay_start = (
                    datetime.now(UTC) - timedelta(minutes=self.config.databento_replay_minutes)
                ).isoformat()
                common = {
                    "dataset": self.config.databento_dataset,
                    "symbols": self.config.databento_symbol,
                    "stype_in": self.config.databento_stype,
                }
                # Databento Standard has verified live 1-second OHLCV access
                # for this account.  Topstep is the authoritative source for
                # the live NQ quote, trade tape, and full DOM.  Do not request
                # mbp-10 here: that is a separate live entitlement and a
                # rejected request must never take the working gateway offline.
                self.live.subscribe(schema="ohlcv-1s", start=replay_start, **common)
                self.live.add_callback(self._enqueue)
                self.live.start()
                backoff = 1.0
                self.live.block_for_close()
                if not self.stop_event.is_set():
                    self._schedule_error("Databento disconnected; reconnecting.")
            except Exception as exc:
                self._schedule_error(f"Databento: {type(exc).__name__}: {exc}")
            finally:
                self.live = None

            if self.stop_event.wait(backoff):
                break
            backoff = min(30.0, backoff * 2)

    async def _record_error(self, message: str) -> None:
        await self.state.set_provider("databento", connected=False, error=message)

    def _schedule_error(self, message: str) -> None:
        if self.loop is None:
            return
        asyncio.run_coroutine_threadsafe(
            self._record_error(message),
            self.loop,
        )

    async def _consume(self) -> None:
        while True:
            record = await self.queue.get()
            try:
                await self._handle(record)
            except Exception as exc:
                await self._record_error(
                    f"Databento record error: {type(exc).__name__}: {exc}"
                )
            finally:
                self.queue.task_done()

    async def _handle(self, record: Any) -> None:
        class_name = type(record).__name__.lower()
        if "errormsg" in class_name or class_name == "error":
            message = str(getattr(record, "err", None) or getattr(record, "message", None) or record)
            await self._record_error(f"Databento: {message}")
            return
        if "symbolmapping" in class_name:
            # Keep the user-facing symbol tied to Topstep's selected contract.
            return
        if "system" in class_name:
            return

        timestamp, epoch = _timestamp(record)
        if "ohlcv" in class_name:
            values = [_price(getattr(record, name, None)) for name in ("open", "high", "low", "close")]
            if any(value is None for value in values):
                return
            await self.state.apply_bar(
                timestamp=timestamp,
                timestamp_epoch=epoch,
                open_price=float(values[0]),
                high=float(values[1]),
                low=float(values[2]),
                close=float(values[3]),
                volume=float(getattr(record, "volume", 0) or 0),
            )
            return
        if "mbp10" in class_name or "mbp_10" in class_name:
            levels: list[dict[str, float]] = []
            for level in list(getattr(record, "levels", []) or [])[:10]:
                bid_price = _price(getattr(level, "bid_px", None))
                ask_price = _price(getattr(level, "ask_px", None))
                if bid_price is None or ask_price is None:
                    continue
                levels.append(
                    {
                        "bidPrice": bid_price,
                        "askPrice": ask_price,
                        "bidSize": float(getattr(level, "bid_sz", 0) or 0),
                        "askSize": float(getattr(level, "ask_sz", 0) or 0),
                    }
                )
            await self.state.apply_depth(timestamp=timestamp, timestamp_epoch=epoch, levels=levels)
            return
        if "trade" in class_name:
            trade_price = _price(getattr(record, "price", None))
            if trade_price is None:
                return
            await self.state.apply_trade(
                timestamp=timestamp,
                timestamp_epoch=epoch,
                price=trade_price,
                size=float(getattr(record, "size", 0) or 0),
                aggressor=_enum_text(getattr(record, "side", "")),
            )
