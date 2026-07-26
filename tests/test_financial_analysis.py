from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from build_kaggle_analysis import profit_direction, safe_growth


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FinancialAnalysisTests(unittest.TestCase):
    def test_published_summary_is_balanced_and_complete(self) -> None:
        summary = pd.read_csv(
            PROJECT_ROOT / "kaggle" / "dataset" / "financial_summary.csv"
        )
        self.assertEqual(len(summary), 8)
        self.assertEqual(summary["company_id"].nunique(), 8)
        self.assertFalse(summary.isna().any().any())
        balance_difference = (
            summary["q1_2026_assets_krw"]
            - summary["q1_2026_liabilities_krw"]
            - summary["q1_2026_equity_krw"]
        ).abs()
        self.assertTrue((balance_difference <= 1).all())

    def test_growth_matches_published_amounts(self) -> None:
        summary = pd.read_csv(
            PROJECT_ROOT / "kaggle" / "dataset" / "financial_summary.csv"
        )
        calculated = (
            (summary["q1_2026_revenue_krw"] - summary["q1_2025_revenue_krw"])
            / summary["q1_2025_revenue_krw"]
            * 100
        )
        self.assertTrue(
            ((calculated - summary["q1_revenue_yoy_pct"]).abs() < 1e-9).all()
        )

    def test_profit_direction_handles_sign_changes(self) -> None:
        self.assertEqual(profit_direction(1, -1), "Turned profitable")
        self.assertEqual(profit_direction(-1, 1), "Turned to loss")
        self.assertEqual(profit_direction(-2, -1), "Deteriorated")
        self.assertEqual(safe_growth(120, 100), 20)


if __name__ == "__main__":
    unittest.main()
