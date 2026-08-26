from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class DailyBar:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def change(self) -> float:
        return self.close - self.open

    @property
    def range(self) -> float:
        return self.high - self.low


class NqHistoryResearch:
    """Lazy, cached Databento daily NQ research for statistical questions."""

    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key
        self._bars: list[DailyBar] | None = None
        self._lock = asyncio.Lock()
        self.last_error: str | None = None

    async def answer(self, question: str) -> dict[str, Any]:
        bars = await self._load()
        result = answer_question(bars, question)
        result["coverage"] = {
            "source": "Databento GLBX.MDP3 · NQ.v.0 · OHLCV-1D",
            "start": bars[0].date,
            "end": bars[-1].date,
            "sessions": len(bars),
        }
        return result

    def health(self) -> dict[str, Any]:
        return {
            "configured": bool(self.api_key),
            "cachedSessions": len(self._bars or []),
            "lastError": self.last_error,
        }

    async def _load(self) -> list[DailyBar]:
        if self._bars:
            return self._bars
        if not self.api_key:
            raise RuntimeError("Koda's 15-year Databento history key is not configured.")

        async with self._lock:
            if self._bars:
                return self._bars
            try:
                bars = await asyncio.to_thread(self._load_sync)
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                raise RuntimeError("Koda could not load the licensed NQ history right now.") from exc
            if len(bars) < 500:
                self.last_error = f"Only {len(bars)} daily sessions returned"
                raise RuntimeError("The historical NQ response did not contain enough sessions.")
            self._bars = bars
            self.last_error = None
            return bars

    def _load_sync(self) -> list[DailyBar]:
        import databento as db

        client = db.Historical(key=self.api_key)
        store = client.timeseries.get_range(
            dataset="GLBX.MDP3",
            symbols="NQ.v.0",
            stype_in="continuous",
            schema="ohlcv-1d",
            start="2010-06-06",
            end=datetime.now(UTC).date().isoformat(),
        )
        frame = store.to_df()
        bars: list[DailyBar] = []
        for index, row in frame.iterrows():
            values = [row.get(name) for name in ("open", "high", "low", "close", "volume")]
            if any(value is None or not math.isfinite(float(value)) for value in values):
                continue
            bars.append(
                DailyBar(
                    date=index.date().isoformat(),
                    open=float(values[0]),
                    high=float(values[1]),
                    low=float(values[2]),
                    close=float(values[3]),
                    volume=max(0.0, float(values[4])),
                )
            )
        return sorted(bars, key=lambda bar: bar.date)


def answer_question(bars: list[DailyBar], question: str) -> dict[str, Any]:
    if not bars:
        raise ValueError("At least one historical session is required")
    normalized = " ".join(question.lower().split())
    weekdays = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
    }
    selected = bars
    scope = "all loaded NQ sessions"
    for name, weekday in weekdays.items():
        if name in normalized:
            selected = [bar for bar in bars if datetime.fromisoformat(bar.date).weekday() == weekday]
            scope = f"historical NQ {name.title()} sessions"
            break
    if not selected:
        selected = bars
        scope = "all loaded NQ sessions"

    green = sum(1 for bar in selected if bar.change > 0)
    red = sum(1 for bar in selected if bar.change < 0)
    green_rate = green / len(selected) * 100
    average_change = sum(bar.change for bar in selected) / len(selected)
    average_range = sum(bar.range for bar in selected) / len(selected)

    if "after green" in normalized or "after an up" in normalized or "after up day" in normalized:
        next_bars = [bars[index + 1] for index in range(len(bars) - 1) if bars[index].change > 0]
        return _conditional_result(next_bars, "sessions after a green NQ day")
    if "after red" in normalized or "after a down" in normalized or "after down day" in normalized:
        next_bars = [bars[index + 1] for index in range(len(bars) - 1) if bars[index].change < 0]
        return _conditional_result(next_bars, "sessions after a red NQ day")
    if "best day" in normalized or "best weekday" in normalized or "day of week" in normalized:
        rows = []
        for name, weekday in weekdays.items():
            day_bars = [bar for bar in bars if datetime.fromisoformat(bar.date).weekday() == weekday]
            if not day_bars:
                continue
            rows.append(
                {
                    "label": name.title(),
                    "greenRate": round(sum(bar.change > 0 for bar in day_bars) / len(day_bars) * 100, 1),
                    "averageChange": round(sum(bar.change for bar in day_bars) / len(day_bars), 2),
                    "averageRange": round(sum(bar.range for bar in day_bars) / len(day_bars), 2),
                    "samples": len(day_bars),
                }
            )
        best = max(rows, key=lambda row: float(row["averageChange"]))
        return {
            "answer": f"{best['label']} has the strongest average open-to-close NQ change in the loaded history: {best['averageChange']:+.2f} points across {best['samples']} sessions.",
            "scope": "weekday comparison",
            "stats": rows,
        }
    if "gap" in normalized:
        gaps = [bars[index].open - bars[index - 1].close for index in range(1, len(bars))]
        positive = sum(gap > 0 for gap in gaps)
        average = sum(gaps) / len(gaps)
        return {
            "answer": f"NQ opened above the prior close in {positive / len(gaps) * 100:.1f}% of {len(gaps):,} measured sessions. The average gap was {average:+.2f} points.",
            "scope": "prior-close to next-open gaps",
            "stats": [
                {"label": "Gap-up rate", "value": round(positive / len(gaps) * 100, 1), "unit": "%"},
                {"label": "Average gap", "value": round(average, 2), "unit": "points"},
            ],
        }
    if "range" in normalized or "volatility" in normalized:
        return {
            "answer": f"The average high-to-low range for {scope} is {average_range:.2f} NQ points across {len(selected):,} sessions.",
            "scope": scope,
            "stats": [
                {"label": "Average range", "value": round(average_range, 2), "unit": "points"},
                {"label": "Sessions", "value": len(selected), "unit": ""},
            ],
        }

    return {
        "answer": f"Across {len(selected):,} {scope}, NQ closed green {green_rate:.1f}% of the time, averaged {average_change:+.2f} open-to-close points, and traveled {average_range:.2f} high-to-low points.",
        "scope": scope,
        "stats": [
            {"label": "Green rate", "value": round(green_rate, 1), "unit": "%"},
            {"label": "Red sessions", "value": red, "unit": ""},
            {"label": "Average change", "value": round(average_change, 2), "unit": "points"},
            {"label": "Average range", "value": round(average_range, 2), "unit": "points"},
        ],
    }


def _conditional_result(bars: list[DailyBar], scope: str) -> dict[str, Any]:
    green_rate = sum(bar.change > 0 for bar in bars) / len(bars) * 100
    average_change = sum(bar.change for bar in bars) / len(bars)
    return {
        "answer": f"Across {len(bars):,} {scope}, the next session finished green {green_rate:.1f}% of the time with an average open-to-close change of {average_change:+.2f} points.",
        "scope": scope,
        "stats": [
            {"label": "Next-session green rate", "value": round(green_rate, 1), "unit": "%"},
            {"label": "Average next change", "value": round(average_change, 2), "unit": "points"},
            {"label": "Samples", "value": len(bars), "unit": ""},
        ],
    }
