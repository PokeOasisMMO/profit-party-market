from __future__ import annotations

import unittest
import time
from datetime import UTC, datetime

from gateway.config import CONFIG
from gateway.market_state import MarketState
from gateway.providers.topstep_market import TopstepMarketFeed, _timestamp


class TopstepMarketFeedTests(unittest.IsolatedAsyncioTestCase):
    def test_dotnet_default_timestamp_falls_back_to_receipt_time(self) -> None:
        _, epoch = _timestamp("0001-01-01T00:00:00+00:00")
        self.assertLess(abs(time.time() - epoch), 2)

    async def test_quote_prefers_current_last_updated_over_default_timestamp(self) -> None:
        state = MarketState(CONFIG)
        feed = TopstepMarketFeed(CONFIG, state)
        current = datetime.now(UTC).isoformat()
        try:
            await feed._process_bridge_message(
                {
                    "kind": "event",
                    "target": "GatewayQuote",
                    "data": {
                        "lastPrice": 25_100.25,
                        "bestBid": 25_100.0,
                        "bestAsk": 25_100.25,
                        "timestamp": "0001-01-01T00:00:00+00:00",
                        "lastUpdated": current,
                    },
                }
            )
            snapshot = await state.snapshot()
            self.assertEqual(snapshot["marketStreams"]["quotes"]["state"], "live")
            self.assertLess(snapshot["providers"]["topstepMarket"]["latencyMs"], 2_000)
        finally:
            await feed.stop()

    async def test_signalr_array_payloads_and_initial_depth_snapshot_enter_market_state(self) -> None:
        state = MarketState(CONFIG)
        feed = TopstepMarketFeed(CONFIG, state)
        timestamp = datetime.now(UTC).isoformat()

        try:
            await feed._process_bridge_message(
                {
                    "kind": "event",
                    "target": "GatewayQuote",
                    "contractId": "CON.F.US.ENQ.Z26",
                    "data": {
                        "lastPrice": 25_100.25,
                        "bestBid": 25_100.0,
                        "bestAsk": 25_100.25,
                        "timestamp": timestamp,
                    },
                }
            )
            await feed._process_bridge_message(
                {
                    "kind": "event",
                    "target": "GatewayTrade",
                    "contractId": "CON.F.US.ENQ.Z26",
                    "data": [{
                        "price": 25_100.25,
                        "volume": 4,
                        "type": 0,
                        "timestamp": timestamp,
                    }],
                }
            )
            await feed._process_bridge_message(
                {
                    "kind": "event",
                    "target": "GatewayDepth",
                    "contractId": "CON.F.US.ENQ.Z26",
                    "data": [
                        {"type": 6, "price": 0, "volume": 0, "currentVolume": 0, "timestamp": timestamp},
                        # ProjectX's initial snapshot uses currentVolume=0;
                        # the real displayed size is in volume.
                        {"price": 25_100.0, "volume": 20, "currentVolume": 0, "type": 2, "timestamp": timestamp},
                        {"price": 25_100.25, "volume": 14, "currentVolume": 0, "type": 1, "timestamp": timestamp},
                    ],
                }
            )

            snapshot = await state.snapshot()
            self.assertEqual(snapshot["instrument"]["price"], 25_100.25)
            self.assertEqual(snapshot["instrument"]["source"], "topstepMarket")
            self.assertEqual(state.trades[-1]["signed_size"], 4)
            self.assertEqual(len(snapshot["bars"]), 1)
            self.assertEqual(state.depth[0]["bidSize"], 20)
            self.assertEqual(state.depth[0]["askSize"], 14)
        finally:
            await feed.stop()

    async def test_signalr_subscription_error_is_exposed_for_the_missing_stream(self) -> None:
        state = MarketState(CONFIG)
        feed = TopstepMarketFeed(CONFIG, state)
        try:
            await feed._process_bridge_message(
                {
                    "kind": "subscription",
                    "stream": "depth",
                    "state": "error",
                    "error": "Market depth entitlement required",
                }
            )
            snapshot = await state.snapshot()
            depth = snapshot["marketStreams"]["depth"]
            self.assertEqual(depth["state"], "error")
            self.assertEqual(depth["error"], "Market depth entitlement required")
        finally:
            await feed.stop()

    async def test_official_bridge_acknowledgement_is_visible_before_first_event(self) -> None:
        state = MarketState(CONFIG)
        feed = TopstepMarketFeed(CONFIG, state)
        try:
            await feed._process_bridge_message(
                {"kind": "subscription", "stream": "trades", "state": "subscribed"}
            )
            snapshot = await state.snapshot()
            self.assertEqual(snapshot["marketStreams"]["trades"]["state"], "subscribed")
        finally:
            await feed.stop()


if __name__ == "__main__":
    unittest.main()
