from __future__ import annotations

import asyncio
import csv
import io
from datetime import UTC, datetime

import httpx

from ..config import GatewayConfig
from ..market_state import MarketState


class TreasuryYieldFeed:
    """Official daily 2Y/10Y par-yield context from the U.S. Treasury.

    Treasury publishes these constant-maturity observations once per business
    day from indicative bid-side quotations near 3:30 PM ET.  They are useful
    as a macro regime prior, but deliberately never masquerade as a live scalp
    trigger.  TLT/IEF/SHY from Alpaca provide the intraday rates proxy.
    """

    def __init__(self, config: GatewayConfig, state: MarketState) -> None:
        self.config = config
        self.state = state
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(20, connect=10),
            headers={"User-Agent": "ProfitParty/1.0 market-context"},
            follow_redirects=True,
        )

    async def start(self) -> None:
        if not self.config.treasury_yields_enabled:
            await self.state.set_treasury_error("Official Treasury yield context is disabled.")
            return
        self.task = asyncio.create_task(self._run(), name="treasury-yields")

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.client.aclose()

    @staticmethod
    def _number(row: dict[str, str], key: str) -> float | None:
        raw = str(row.get(key, "")).strip()
        if not raw or raw.upper() in {"N/A", "NA"}:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    async def _refresh(self) -> None:
        year = datetime.now(UTC).year
        url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all"
        response = await self.client.get(
            url,
            params={
                "type": "daily_treasury_yield_curve",
                "field_tdr_date_value": str(year),
                "page": "",
                "_format": "csv",
            },
        )
        response.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))))
        valid = [
            row
            for row in rows
            if self._number(row, "2 Yr") is not None and self._number(row, "10 Yr") is not None
        ]
        if not valid:
            raise RuntimeError("Treasury CSV contained no valid 2Y/10Y observations")

        def row_date(row: dict[str, str]) -> datetime:
            raw = str(row.get("Date") or "").strip()
            for pattern in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(raw, pattern).replace(tzinfo=UTC)
                except ValueError:
                    continue
            return datetime.min.replace(tzinfo=UTC)

        # Treasury currently returns newest-first, but sorting keeps the feed
        # correct if the upstream CSV ordering changes.
        valid.sort(key=row_date)
        latest = valid[-1]
        previous = valid[-2] if len(valid) > 1 else None
        two_year = self._number(latest, "2 Yr")
        ten_year = self._number(latest, "10 Yr")
        previous_two = self._number(previous, "2 Yr") if previous else None
        previous_ten = self._number(previous, "10 Yr") if previous else None
        assert two_year is not None and ten_year is not None

        await self.state.set_treasury_yields(
            as_of=str(latest.get("Date") or ""),
            two_year=two_year,
            ten_year=ten_year,
            two_year_change_bps=(two_year - previous_two) * 100 if previous_two is not None else None,
            ten_year_change_bps=(ten_year - previous_ten) * 100 if previous_ten is not None else None,
        )

    async def _run(self) -> None:
        backoff_seconds = 30.0
        while not self.stop_event.is_set():
            try:
                await self._refresh()
                backoff_seconds = float(self.config.treasury_poll_minutes * 60)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.state.set_treasury_error(f"Treasury yields: {type(exc).__name__}: {exc}")
                backoff_seconds = min(900.0, max(30.0, backoff_seconds * 2))

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=backoff_seconds)
            except TimeoutError:
                pass
