from __future__ import annotations

import unittest
from datetime import UTC, datetime

from gateway.market_state import _nq_session_start_epoch, _session_vwap


def epoch(value: str) -> float:
    return datetime.fromisoformat(value).astimezone(UTC).timestamp()


def bar(
    timestamp: str,
    *,
    high: float,
    low: float,
    close: float,
    volume: float,
) -> dict[str, float]:
    return {
        "epoch": epoch(timestamp),
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class SessionVwapTests(unittest.TestCase):
    def test_uses_volume_weighted_hlc3_for_current_nq_session_only(self) -> None:
        reference = epoch("2026-08-26T12:00:00+00:00")
        bars = [
            bar(
                "2026-08-25T21:59:00+00:00",
                high=101,
                low=99,
                close=100,
                volume=1_000,
            ),
            bar("2026-08-25T22:00:00+00:00", high=12, low=8, close=10, volume=2),
            bar("2026-08-25T22:01:00+00:00", high=22, low=18, close=20, volume=1),
            bar("2026-08-25T22:02:00+00:00", high=999, low=999, close=999, volume=0),
            bar(
                "2026-08-26T12:01:00+00:00",
                high=999,
                low=999,
                close=999,
                volume=1_000,
            ),
        ]

        vwap, volume, count, session_start = _session_vwap(bars, reference)

        self.assertAlmostEqual(vwap or 0, 40 / 3)
        self.assertEqual(volume, 3)
        self.assertEqual(count, 2)
        self.assertEqual(session_start, "2026-08-25T22:00:00+00:00")

    def test_session_start_is_dst_safe(self) -> None:
        winter_reference = epoch("2026-01-14T15:00:00+00:00")
        summer_reference = epoch("2026-07-14T15:00:00+00:00")

        self.assertEqual(
            _nq_session_start_epoch(winter_reference),
            epoch("2026-01-13T23:00:00+00:00"),
        )
        self.assertEqual(
            _nq_session_start_epoch(summer_reference),
            epoch("2026-07-13T22:00:00+00:00"),
        )

    def test_no_positive_session_volume_returns_no_vwap(self) -> None:
        reference = epoch("2026-08-26T12:00:00+00:00")
        vwap, volume, count, _ = _session_vwap(
            [
                bar(
                    "2026-08-25T22:00:00+00:00",
                    high=12,
                    low=8,
                    close=10,
                    volume=0,
                )
            ],
            reference,
        )

        self.assertIsNone(vwap)
        self.assertEqual(volume, 0)
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
