from __future__ import annotations

import unittest

from gateway.nq_history import DailyBar, answer_question


class NqHistoryQuestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bars = [
            DailyBar("2026-08-24", 100, 112, 98, 110, 1_000),
            DailyBar("2026-08-25", 110, 113, 99, 102, 900),
            DailyBar("2026-08-26", 102, 121, 101, 118, 1_200),
            DailyBar("2026-08-27", 118, 120, 108, 112, 1_100),
            DailyBar("2026-08-28", 112, 118, 109, 116, 800),
        ]

    def test_overview_uses_observed_sessions(self) -> None:
        result = answer_question(self.bars, "How often is NQ green?")
        self.assertIn("60.0%", result["answer"])
        self.assertEqual(result["stats"][0]["value"], 60.0)

    def test_range_question_can_filter_weekday(self) -> None:
        result = answer_question(self.bars, "What is the average Wednesday range?")
        self.assertIn("20.00", result["answer"])
        self.assertEqual(result["scope"], "historical NQ Wednesday sessions")

    def test_after_green_uses_only_next_sessions(self) -> None:
        result = answer_question(self.bars, "What happens after green days?")
        self.assertEqual(result["stats"][2]["value"], 2)


if __name__ == "__main__":
    unittest.main()
