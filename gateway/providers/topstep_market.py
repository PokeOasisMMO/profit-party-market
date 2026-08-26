from __future__ import annotations

import asyncio
import json
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from ..config import GatewayConfig
from ..market_state import MarketState


def _timestamp(*values: Any) -> tuple[str, float]:
    """Return the first plausible ProjectX timestamp, or local receipt time.

    ProjectX occasionally includes the .NET default date (year 1) in one of a
    live event's timestamp fields.  Treating that sentinel as an exchange time
    produced multi-trillion-millisecond latency and made an otherwise live
    stream immediately stale.  Quotes also include ``lastUpdated``, so callers
    can pass both fields in freshness order.
    """
    now = datetime.now(UTC)
    minimum_epoch = datetime(2000, 1, 1, tzinfo=UTC).timestamp()
    maximum_epoch = now.timestamp() + 300

    for value in values:
        moment: datetime | None = None
        if isinstance(value, (int, float)):
            try:
                epoch = float(value)
                # Accept seconds, milliseconds, microseconds, or nanoseconds.
                while abs(epoch) > 10_000_000_000:
                    epoch /= 1_000
                moment = datetime.fromtimestamp(epoch, UTC)
            except (OSError, OverflowError, ValueError):
                moment = None
        else:
            text = str(value or "").strip()
            if text:
                normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
                try:
                    moment = datetime.fromisoformat(normalized)
                except ValueError:
                    moment = None
                if moment is not None:
                    if moment.tzinfo is None:
                        moment = moment.replace(tzinfo=UTC)
                    moment = moment.astimezone(UTC)

        if moment is not None and minimum_epoch <= moment.timestamp() <= maximum_epoch:
            return moment.isoformat(), moment.timestamp()

    return now.isoformat(), now.timestamp()


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class TopstepMarketFeed:
    """Topstep/ProjectX NQ market adapter.

    Historical one-second bars come from the REST History endpoint. Live quote,
    trade, and DOM events come from the ProjectX SignalR market hub. The adapter
    never manufactures a price or falls back to a timer-driven demo feed.
    """

    API_ROOT = "https://api.topstepx.com/api"
    BRIDGE_PATH = Path(__file__).resolve().parent.parent / "projectx_market_bridge.mjs"

    def __init__(self, config: GatewayConfig, state: MarketState) -> None:
        self.config = config
        self.state = state
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(15, connect=8))
        self.token: str | None = None
        self.contract: dict[str, Any] | None = None
        self.bridge_process: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        if not self.config.topstep_username or not self.config.topstep_api_key:
            await self.state.set_provider(
                "topstepMarket",
                connected=False,
                error="Topstep username or API key is missing from gateway/.env.",
            )
            return
        self.task = asyncio.create_task(self._run(), name="topstep-nq-market")

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self._stop_bridge()
        await self.client.aclose()

    async def _authenticate(self) -> None:
        response = await self.client.post(
            f"{self.API_ROOT}/Auth/loginKey",
            json={
                "userName": self.config.topstep_username,
                "apiKey": self.config.topstep_api_key,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success") or not payload.get("token"):
            detail = payload.get("errorMessage") or json.dumps(payload, separators=(",", ":"))
            raise RuntimeError(f"Topstep authentication failed: {detail}")
        self.token = str(payload["token"])

    async def _post(
        self,
        path: str,
        body: dict[str, Any],
        *,
        retry_auth: bool = True,
    ) -> dict[str, Any]:
        if not self.token:
            await self._authenticate()
        response = await self.client.post(
            f"{self.API_ROOT}/{path.lstrip('/')}",
            json=body,
            headers={"Authorization": f"Bearer {self.token}"},
        )
        if response.status_code == 401 and retry_auth:
            self.token = None
            await self._authenticate()
            return await self._post(path, body, retry_auth=False)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success", False):
            detail = payload.get("errorMessage") or json.dumps(payload, separators=(",", ":"))
            raise RuntimeError(f"Topstep {path} failed: {detail}")
        return payload

    async def _select_contract(self) -> dict[str, Any]:
        if self.config.topstep_contract_id:
            return {
                "id": self.config.topstep_contract_id,
                "name": self.config.topstep_contract_id,
                "activeContract": True,
            }

        payload = await self._post(
            "Contract/search",
            {
                "searchText": self.config.topstep_contract_search,
                "live": self.config.topstep_market_live,
            },
        )
        contracts = list(payload.get("contracts") or [])
        nq_contracts = [
            item
            for item in contracts
            if str(item.get("symbolId", "")).upper() == "F.US.ENQ"
            or "E-MINI NASDAQ-100" in str(item.get("description", "")).upper()
        ]
        selected = next((item for item in nq_contracts if item.get("activeContract")), None)
        selected = selected or (nq_contracts[0] if nq_contracts else None)
        if not selected:
            raise RuntimeError(
                "No active E-mini Nasdaq-100 contract was returned. "
                "Set TOPSTEP_CONTRACT_ID to the current NQ contract ID if needed."
            )
        return selected

    async def _seed_history(self, contract_id: str, *, lookback_minutes: int | None = None) -> None:
        end = datetime.now(UTC)
        minutes = max(1, lookback_minutes or self.config.topstep_history_minutes)
        start = end - timedelta(minutes=minutes)
        limit = min(20_000, max(120, minutes * 60 + 30))
        payload = await self._post(
            "History/retrieveBars",
            {
                "contractId": contract_id,
                "live": self.config.topstep_market_live,
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
                "unit": 1,
                "unitNumber": 1,
                "limit": limit,
                "includePartialBar": True,
            },
        )
        bars = sorted(
            list(payload.get("bars") or []),
            key=lambda bar: _timestamp(bar.get("t"))[1],
        )
        for bar in bars:
            timestamp, epoch = _timestamp(bar.get("t"))
            values = [_number(bar.get(key)) for key in ("o", "h", "l", "c", "v")]
            if any(value is None for value in values):
                continue
            open_price, high, low, close, volume = values
            await self.state.apply_bar(
                timestamp=timestamp,
                timestamp_epoch=epoch,
                open_price=float(open_price),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=float(volume),
                provider_name="topstepMarket",
            )

    async def _seed_context_history(self, contract_id: str, *, lookback_minutes: int = 10_080) -> None:
        """Load real one-minute bars for Koda's 5m through 4h structure brain."""
        end = datetime.now(UTC)
        minutes = max(480, lookback_minutes)
        start = end - timedelta(minutes=minutes)
        payload = await self._post(
            "History/retrieveBars",
            {
                "contractId": contract_id,
                "live": self.config.topstep_market_live,
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
                "unit": 2,
                "unitNumber": 1,
                "limit": min(20_000, minutes + 120),
                "includePartialBar": True,
            },
        )
        bars = sorted(
            list(payload.get("bars") or []),
            key=lambda bar: _timestamp(bar.get("t"))[1],
        )
        for bar in bars:
            timestamp, epoch = _timestamp(bar.get("t"))
            values = [_number(bar.get(key)) for key in ("o", "h", "l", "c", "v")]
            if any(value is None for value in values):
                continue
            open_price, high, low, close, volume = values
            await self.state.apply_context_bar(
                timestamp=timestamp,
                timestamp_epoch=epoch,
                open_price=float(open_price),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=float(volume),
            )

    async def _refresh_history(self, contract_id: str) -> None:
        """Keep Topstep's real partial one-second bar current without using trade events."""
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=2.0)
                return
            except TimeoutError:
                pass

            try:
                await self._seed_history(contract_id, lookback_minutes=1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.state.set_market_stream(
                    "bars",
                    state="error",
                    error=f"Topstep bar refresh: {type(exc).__name__}: {exc}",
                    source="topstepMarket",
                )

    async def _refresh_context_history(self, contract_id: str) -> None:
        """Refresh the rolling minute context without touching live feed health."""
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=30.0)
                return
            except TimeoutError:
                pass
            try:
                await self._seed_context_history(contract_id, lookback_minutes=180)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Higher-timeframe context is supplemental. A temporary REST
                # failure must never disconnect the live quote/tape bridge.
                continue

    async def _process_bridge_message(self, message: dict[str, Any]) -> None:
        kind = str(message.get("kind", ""))
        if kind == "subscription":
            stream = str(message.get("stream", ""))
            state = str(message.get("state", "waiting"))
            if stream in {"quotes", "trades", "depth"}:
                await self.state.set_market_stream(
                    stream,
                    state="error" if state == "error" else "subscribed",
                    error=str(message.get("error")) if message.get("error") else None,
                )
            return

        if kind == "connection":
            connected = message.get("state") == "connected"
            await self.state.set_provider(
                "topstepMarket",
                connected=connected,
                error=str(message.get("error")) if message.get("error") else None,
                touched=connected,
            )
            return

        if kind == "fatal":
            raise RuntimeError(str(message.get("error") or "ProjectX SignalR bridge stopped"))

        if kind != "event":
            return

        target = str(message.get("target", "")).lower()
        raw_data = message.get("data")
        # ProjectX sends quote updates as objects, but emits trade and DOM
        # snapshots/updates as arrays.  Treat both shapes uniformly so an
        # initial DOM snapshot is never silently discarded.
        events = raw_data if isinstance(raw_data, list) else [raw_data]
        events = [data for data in events if isinstance(data, dict)]
        if not events:
            return

        if target == "gatewayquote":
            for data in events:
                # `timestamp` can be ProjectX's year-1 placeholder while
                # `lastUpdated` contains the actual current exchange time.
                timestamp, epoch = _timestamp(data.get("lastUpdated"), data.get("timestamp"))
                await self.state.apply_quote(
                    timestamp=timestamp,
                    timestamp_epoch=epoch,
                    last_price=_number(data.get("lastPrice")),
                    bid=_number(data.get("bestBid")),
                    ask=_number(data.get("bestAsk")),
                    provider_name="topstepMarket",
                )
            return

        if target == "gatewaytrade":
            for data in events:
                timestamp, epoch = _timestamp(data.get("timestamp"))
                price = _number(data.get("price"))
                size = _number(data.get("volume"))
                if price is None or size is None:
                    continue
                await self.state.apply_trade(
                    timestamp=timestamp,
                    timestamp_epoch=epoch,
                    price=price,
                    size=size,
                    aggressor="BUY" if int(data.get("type", -1) or 0) == 0 else "SELL",
                    provider_name="topstepMarket",
                    build_one_second_bar=True,
                )
            return

        if target == "gatewaydepth":
            for data in events:
                timestamp, epoch = _timestamp(data.get("timestamp"))
                # In the initial ProjectX depth snapshot, `volume` is the
                # displayed size while `currentVolume` is zero.  `volume` is
                # therefore authoritative whenever it is supplied.
                volume = _number(data.get("volume"))
                current_volume = volume if volume is not None else _number(data.get("currentVolume"))
                await self.state.apply_depth_update(
                    timestamp=timestamp,
                    timestamp_epoch=epoch,
                    depth_type=int(data.get("type", 0) or 0),
                    price=_number(data.get("price")),
                    current_volume=current_volume,
                    provider_name="topstepMarket",
                )

    async def _stop_bridge(self) -> None:
        process = self.bridge_process
        self.bridge_process = None
        if not process or process.returncode is not None:
            return
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            await process.wait()

    async def _connect_market(self, contract_id: str) -> None:
        if not self.token:
            await self._authenticate()
        node = shutil.which("node")
        if not node:
            raise RuntimeError("Node.js is required for the official ProjectX SignalR connection.")
        if not self.BRIDGE_PATH.exists():
            raise RuntimeError("ProjectX SignalR bridge file is missing.")

        environment = os.environ.copy()
        environment["PROJECTX_SESSION_TOKEN"] = str(self.token)
        environment["PROJECTX_CONTRACT_ID"] = contract_id
        self.bridge_process = await asyncio.create_subprocess_exec(
            node,
            str(self.BRIDGE_PATH),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        process = self.bridge_process
        if process.stdout is None:
            raise RuntimeError("ProjectX SignalR bridge did not expose an event stream.")

        try:
            while not self.stop_event.is_set():
                raw_line = await process.stdout.readline()
                if not raw_line:
                    break
                try:
                    message = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if isinstance(message, dict):
                    await self._process_bridge_message(message)

            return_code = await process.wait()
            if not self.stop_event.is_set() and return_code != 0:
                detail = ""
                if process.stderr is not None:
                    detail = (await process.stderr.read()).decode("utf-8", errors="replace").strip()
                raise RuntimeError(detail or f"ProjectX SignalR bridge exited with code {return_code}")
        finally:
            await self._stop_bridge()

    async def _run(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                await self._authenticate()
                self.contract = await self._select_contract()
                contract_id = str(self.contract["id"])
                display_name = str(self.contract.get("name") or contract_id)
                await self.state.apply_symbol_mapping(display_name)
                await self._seed_context_history(contract_id)
                await self._seed_history(contract_id)
                backoff = 1.0
                history_task = asyncio.create_task(
                    self._refresh_history(contract_id),
                    name="topstep-nq-history-refresh",
                )
                context_task = asyncio.create_task(
                    self._refresh_context_history(contract_id),
                    name="topstep-nq-context-refresh",
                )
                try:
                    await self._connect_market(contract_id)
                finally:
                    history_task.cancel()
                    context_task.cancel()
                    await asyncio.gather(history_task, context_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.token = None
                await self.state.set_provider(
                    "topstepMarket",
                    connected=False,
                    error=f"Topstep NQ: {type(exc).__name__}: {exc}",
                )

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=backoff)
            except TimeoutError:
                pass
            backoff = min(30.0, backoff * 2)
