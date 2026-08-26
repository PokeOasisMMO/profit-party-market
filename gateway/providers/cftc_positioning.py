from __future__ import annotations

import asyncio
from typing import Any

import httpx

from ..config import GatewayConfig
from ..market_state import MarketState


class CftcPositioningFeed:
    """Weekly NQ positioning from the CFTC public Socrata API.

    The report is Tuesday open interest published on Friday. TREE treats it as
    a slow positioning prior only; it can never delay, create, or time an
    intraday entry. The endpoint is public and currently requires no token.
    """

    DATASET_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
    NQ_CONTRACT_CODE = "209742"

    def __init__(self, config: GatewayConfig, state: MarketState) -> None:
        self.config = config
        self.state = state
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(20, connect=10),
            headers={"User-Agent": "ProfitParty/1.0 causal-positioning"},
            follow_redirects=True,
        )

    async def start(self) -> None:
        if not self.config.cftc_positioning_enabled:
            await self.state.set_cftc_error("CFTC positioning context is disabled.")
            return
        self.task = asyncio.create_task(self._run(), name="cftc-positioning")

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.client.aclose()

    @staticmethod
    def _number(row: dict[str, Any], key: str) -> float | None:
        raw = row.get(key)
        if raw is None or str(raw).strip() in {"", "."}:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @classmethod
    def parse_rows(cls, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            raise RuntimeError("CFTC returned no NASDAQ-100 positioning rows")
        latest = rows[0]
        previous = rows[1] if len(rows) > 1 else {}

        asset_long = cls._number(latest, "asset_mgr_positions_long_all")
        asset_short = cls._number(latest, "asset_mgr_positions_short_all")
        leveraged_long = cls._number(latest, "lev_money_positions_long_all")
        leveraged_short = cls._number(latest, "lev_money_positions_short_all")
        if None in {asset_long, asset_short, leveraged_long, leveraged_short}:
            raise RuntimeError("CFTC positioning row is missing required TFF fields")

        previous_asset_long = cls._number(previous, "asset_mgr_positions_long_all")
        previous_asset_short = cls._number(previous, "asset_mgr_positions_short_all")
        previous_leveraged_long = cls._number(previous, "lev_money_positions_long_all")
        previous_leveraged_short = cls._number(previous, "lev_money_positions_short_all")
        asset_net = float(asset_long) - float(asset_short)
        leveraged_net = float(leveraged_long) - float(leveraged_short)
        previous_asset_net = (
            float(previous_asset_long) - float(previous_asset_short)
            if previous_asset_long is not None and previous_asset_short is not None
            else None
        )
        previous_leveraged_net = (
            float(previous_leveraged_long) - float(previous_leveraged_short)
            if previous_leveraged_long is not None and previous_leveraged_short is not None
            else None
        )

        as_of = str(
            latest.get("report_date_as_yyyy_mm_dd")
            or latest.get("report_date_as_mm_dd_yyyy")
            or latest.get("as_of_date_in_form_yymmdd")
            or ""
        )
        return {
            "asOf": as_of,
            "market": str(latest.get("market_and_exchange_names") or "NASDAQ-100 STOCK INDEX (MINI)"),
            "openInterest": cls._number(latest, "open_interest_all"),
            "assetManagerLong": asset_long,
            "assetManagerShort": asset_short,
            "assetManagerNet": asset_net,
            "assetManagerNetChange": asset_net - previous_asset_net if previous_asset_net is not None else None,
            "leveragedLong": leveraged_long,
            "leveragedShort": leveraged_short,
            "leveragedNet": leveraged_net,
            "leveragedNetChange": leveraged_net - previous_leveraged_net if previous_leveraged_net is not None else None,
        }

    async def _refresh(self) -> None:
        response = await self.client.get(
            self.DATASET_URL,
            params={
                "$where": f"cftc_contract_market_code='{self.NQ_CONTRACT_CODE}'",
                "$order": "report_date_as_yyyy_mm_dd DESC",
                "$limit": "2",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError("CFTC response was not a row list")
        await self.state.set_cftc_positioning(self.parse_rows(payload))

    async def _run(self) -> None:
        backoff_seconds = 30.0
        while not self.stop_event.is_set():
            try:
                await self._refresh()
                backoff_seconds = float(self.config.cftc_poll_minutes * 60)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.state.set_cftc_error(f"CFTC positioning: {type(exc).__name__}: {exc}")
                backoff_seconds = min(900.0, max(30.0, backoff_seconds * 2))

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=backoff_seconds)
            except TimeoutError:
                pass
