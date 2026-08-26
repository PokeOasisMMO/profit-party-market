from __future__ import annotations

import unittest
from datetime import UTC, datetime

from gateway.discord_newsroom import (
    classify_news,
    is_relevant_article,
    news_suggestion,
    parse_rss_items,
    period_stats,
)


class DiscordNewsroomTests(unittest.TestCase):
    def test_meta_and_macro_news_are_relevant(self) -> None:
        watch = {"QQQ", "META", "NVDA"}
        self.assertTrue(is_relevant_article({"symbols": ["META"]}, watch))
        self.assertTrue(
            is_relevant_article(
                {"headline": "Federal Reserve releases FOMC decision", "symbols": []},
                watch,
            )
        )
        self.assertFalse(
            is_relevant_article(
                {"headline": "Small restaurant opens a new location", "symbols": ["FOOD"]},
                watch,
            )
        )

    def test_headline_classification_is_conservative(self) -> None:
        self.assertEqual(classify_news({"headline": "META beats estimates and raises guidance"}), "BULLISH")
        self.assertEqual(classify_news({"headline": "Chipmaker cuts guidance on weak demand"}), "BEARISH")
        self.assertEqual(
            classify_news({"headline": "Federal Reserve statement released", "macro": True}),
            "CAUTION",
        )

    def test_news_suggestion_requires_price_confirmation(self) -> None:
        snapshot = {
            "stale": False,
            "ready": True,
            "metrics": {"bias": "BULLISH"},
            "levels": {"trigger": 22_100.25, "invalidation": 22_090.0},
        }
        text = news_suggestion(
            {"headline": "META beats estimates and raises guidance"},
            snapshot,
        )
        self.assertIn("News and price align bullish", text)
        self.assertIn("22,100.25", text)
        self.assertIn("22,090.00", text)

    def test_period_stats_use_real_one_hour_bars(self) -> None:
        snapshot = {
            "timeframes": {
                "1h": [
                    {"ts": "2026-08-26T10:00:00Z", "open": 100, "high": 105, "low": 98, "close": 103},
                    {"ts": "2026-08-26T11:00:00Z", "open": 103, "high": 112, "low": 102, "close": 110},
                ]
            }
        }
        stats = period_stats(snapshot, hours=24, now=datetime(2026, 8, 26, 12, tzinfo=UTC))
        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertEqual(stats["change"], 10)
        self.assertEqual(stats["range"], 14)

    def test_official_rss_items_keep_source_links(self) -> None:
        payload = b"""<?xml version="1.0"?>
        <rss><channel><item>
          <title>CPI news release</title>
          <link>https://www.bls.gov/news.release/cpi.nr0.htm</link>
          <description><![CDATA[<p>Official CPI summary.</p>]]></description>
          <pubDate>Wed, 26 Aug 2026 08:30:00 -0400</pubDate>
          <guid>cpi-2026-08</guid>
        </item></channel></rss>"""
        articles = parse_rss_items(
            payload,
            source="U.S. Bureau of Labor Statistics",
            category="CPI",
        )
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["headline"], "CPI news release")
        self.assertEqual(articles[0]["summary"], "Official CPI summary.")
        self.assertEqual(articles[0]["category"], "CPI")


if __name__ == "__main__":
    unittest.main()
