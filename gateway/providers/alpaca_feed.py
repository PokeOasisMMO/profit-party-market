from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import websockets

from ..config import GatewayConfig
from ..market_state import MarketState


class AlpacaFeed:
    def __init__(self, config: GatewayConfig, state: MarketState) -> None:
        self.config = config
        self.state = state
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(12, connect=8))

    async def start(self) -> None:
        if not self.config.alpaca_api_key_id or not self.config.alpaca_api_secret_key:
            await self.state.set_provider(
                "alpaca",
                connected=False,
                error="Alpaca API credentials are missing from gateway/.env.",
            )
            return
        self.task = asyncio.create_task(self._run(), name="alpaca-live")

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.client.aclose()

    async def _seed_latest(self) -> None:
        """Load real snapshots and each symbol's session open.

        Using the latest minute bar's open as the reference would turn a
        one-minute move into a fake daily change. Alpaca snapshots include the
        current daily bar, which keeps the NQ leadership vote honest.
        """
        symbols = ",".join(self.config.alpaca_symbols)
        headers = {
            "APCA-API-KEY-ID": self.config.alpaca_api_key_id,
            "APCA-API-SECRET-KEY": self.config.alpaca_api_secret_key,
        }
        params = {"symbols": symbols, "feed": self.config.alpaca_feed}
        response = await self.client.get(
            "https://data.alpaca.markets/v2/stocks/snapshots",
            params=params,
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        snapshots = dict(payload.get("snapshots") or payload)
        for symbol, item in snapshots.items():
            if not isinstance(item, dict):
                continue
            daily = dict(item.get("dailyBar") or {})
            reference_open = daily.get("o")
            quote = dict(item.get("latestQuote") or {})
            trade = dict(item.get("latestTrade") or {})
            minute = dict(item.get("minuteBar") or {})
            if quote:
                await self.state.apply_equity_event({"T": "q", "S": symbol, "referenceOpen": reference_open, **quote})
            if trade:
                await self.state.apply_equity_event({"T": "t", "S": symbol, "referenceOpen": reference_open, **trade})
            elif minute:
                await self.state.apply_equity_event({"T": "b", "S": symbol, "referenceOpen": reference_open, **minute})

    async def _run(self) -> None:
        backoff = 1.0
        url = f"wss://stream.data.alpaca.markets/v2/{self.config.alpaca_feed}"
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(
                    url,
                    open_timeout=12,
                    ping_interval=20,
                    ping_timeout=12,
                    close_timeout=5,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    await websocket.send(
                        json.dumps(
                            {
                                "action": "auth",
                                "key": self.config.alpaca_api_key_id,
                                "secret": self.config.alpaca_api_secret_key,
                            }
                        )
                    )
                    authenticated = False
                    auth_events: list[dict[str, Any]] = []
                    for _ in range(3):
                        auth_events = await self._receive_events(websocket)
                        for event in auth_events:
                            if event.get("T") == "error":
                                raise RuntimeError(
                                    f"Alpaca authentication failed: {event.get('msg') or event}"
                                )
                            if event.get("T") == "success" and event.get("msg") == "authenticated":
                                authenticated = True
                                break
                        if authenticated:
                            break
                    if not authenticated:
                        raise RuntimeError(f"Alpaca authentication failed: {auth_events}")

                    symbols = list(self.config.alpaca_symbols)
                    await websocket.send(
                        json.dumps(
                            {
                                "action": "subscribe",
                                "trades": symbols,
                                "quotes": symbols,
                                "bars": symbols,
                                "updatedBars": symbols,
                            }
                        )
                    )
                    await self.state.set_provider("alpaca", connected=True, error=None, touched=True)
                    try:
                        await self._seed_latest()
                    except httpx.HTTPError as exc:
                        # The websocket is still valid; only the after-hours bootstrap is unavailable.
                        await self.state.set_provider("alpaca", connected=True, error=f"Alpaca latest-context bootstrap unavailable: {exc}")
                    backoff = 1.0

                    async for message in websocket:
                        payload = json.loads(message)
                        events: list[dict[str, Any]] = payload if isinstance(payload, list) else [payload]
                        for event in events:
                            event_type = str(event.get("T", ""))
                            if event_type == "error":
                                raise RuntimeError(str(event.get("msg") or event))
                            if event_type in {"q", "t", "b", "u", "d"}:
                                await self.state.apply_equity_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.state.set_provider(
                    "alpaca",
                    connected=False,
                    error=f"Alpaca: {type(exc).__name__}: {exc}",
                )

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=backoff)
            except TimeoutError:
                pass
            backoff = min(30.0, backoff * 2)

    @staticmethod
    async def _receive_events(websocket: Any) -> list[dict[str, Any]]:
        message = await asyncio.wait_for(websocket.recv(), timeout=12)
        payload = json.loads(message)
        return payload if isinstance(payload, list) else [payload]
