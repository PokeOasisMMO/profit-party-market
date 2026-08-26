from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from gateway.discord_bot import (
    KodaCommands,
    freshness,
    nq_session,
    percent,
    price,
    vwap_relation,
)


class DiscordBotTests(unittest.TestCase):
    def test_vip_command_catalog_is_complete(self) -> None:
        names = {command.name for command in KodaCommands.__cog_app_commands__}
        self.assertEqual(
            names,
            {
                "nq",
                "setup",
                "levels",
                "flow",
                "session",
                "koda",
                "stats",
                "website",
                "news",
                "daily",
                "weekly",
                "meta",
            },
        )

    def test_market_values_render_without_fake_numbers(self) -> None:
        self.assertEqual(price(22123.25), "22,123.25")
        self.assertEqual(price(None), "—")
        self.assertEqual(percent(78.4), "78%")
        self.assertEqual(percent(None), "—")

    def test_feed_freshness_is_explicit(self) -> None:
        self.assertEqual(freshness({"stale": False}), "LIVE")
        self.assertEqual(freshness({"stale": True, "staleAgeMs": 12_500}), "STALE • 12.5s old")

    def test_vwap_relation_uses_nq_points(self) -> None:
        self.assertEqual(vwap_relation(22_110.25, 22_100.0), "10.25 points above VWAP")
        self.assertEqual(vwap_relation(None, 22_100.0), "waiting on VWAP")

    def test_session_labels_use_new_york_time(self) -> None:
        et = ZoneInfo("America/New_York")
        self.assertEqual(nq_session(datetime(2026, 8, 26, 9, 30, tzinfo=et))[0], "NEW YORK OPEN")
        self.assertEqual(nq_session(datetime(2026, 8, 26, 19, 0, tzinfo=et))[0], "ASIA SESSION")


if __name__ == "__main__":
    unittest.main()
