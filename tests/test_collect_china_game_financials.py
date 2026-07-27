from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from collect_china_game_financials import (
    DEFAULT_COMPANIES_CONFIG,
    DEFAULT_RELEASES_CONFIG,
    add_calculated_growth,
    load_json,
    parse_numeric,
    run_pipeline,
    validate_configs,
)


class FakeResult:
    def __init__(self, fields: list[str], rows: list[list[str]]) -> None:
        self.fields = fields
        self.rows = rows
        self.error_code = "0"
        self.error_msg = "success"
        self.index = -1

    def next(self) -> bool:
        self.index += 1
        return self.index < len(self.rows)

    def get_row_data(self) -> list[str]:
        return self.rows[self.index]


class FakeLoginResult:
    error_code = "0"
    error_msg = "success"


class FakeBaoStock:
    def __init__(self) -> None:
        self.logged_out = False

    def login(self) -> FakeLoginResult:
        return FakeLoginResult()

    def logout(self) -> FakeLoginResult:
        self.logged_out = True
        return FakeLoginResult()

    @staticmethod
    def _result(code: str, year: int, extra: tuple[str, str]) -> FakeResult:
        field, value = extra
        return FakeResult(
            ["code", "pubDate", "statDate", field],
            [[code, f"{year + 1}-04-30", f"{year}-12-31", value]],
        )

    def query_profit_data(self, code: str, year: int, quarter: int) -> FakeResult:
        return FakeResult(
            ["code", "pubDate", "statDate", "MBRevenue", "netProfit", "roeAvg"],
            [[code, f"{year + 1}-04-30", f"{year}-12-31", str(year * 100), str(year * 10), "0.1"]],
        )

    def query_operation_data(self, code: str, year: int, quarter: int) -> FakeResult:
        return self._result(code, year, ("AssetTurnRatio", "0.5"))

    def query_growth_data(self, code: str, year: int, quarter: int) -> FakeResult:
        return self._result(code, year, ("YOYNI", "0.2"))

    def query_balance_data(self, code: str, year: int, quarter: int) -> FakeResult:
        return self._result(code, year, ("currentRatio", "2.0"))

    def query_cash_flow_data(self, code: str, year: int, quarter: int) -> FakeResult:
        return self._result(code, year, ("CFOToNP", "1.2"))


class ChinaGameCollectorTests(unittest.TestCase):
    def test_repository_configs_cover_every_company_year(self) -> None:
        companies = load_json(DEFAULT_COMPANIES_CONFIG)
        releases = load_json(DEFAULT_RELEASES_CONFIG)
        validate_configs(companies, releases)
        self.assertEqual(len(companies["companies"]), 8)
        self.assertEqual(len(releases["releases"]), 32)
        self.assertEqual(
            sum(c["financial_collection_method"] == "baostock" for c in companies["companies"]),
            5,
        )

    def test_parse_numeric_handles_missing_and_commas(self) -> None:
        self.assertEqual(parse_numeric("1,234.5"), 1234.5)
        self.assertIsNone(parse_numeric("--"))
        self.assertIsNone(parse_numeric("not-a-number"))

    def test_calculates_growth_from_absolute_values(self) -> None:
        rows = [
            {"company_id": "x", "fiscal_year": 2022, "profit_MBRevenue": 100, "profit_netProfit": 20},
            {"company_id": "x", "fiscal_year": 2023, "profit_MBRevenue": 125, "profit_netProfit": 10},
        ]
        add_calculated_growth(rows)
        self.assertIsNone(rows[0]["revenue_yoy_calculated"])
        self.assertAlmostEqual(rows[1]["revenue_yoy_calculated"], 0.25)
        self.assertAlmostEqual(rows[1]["net_profit_yoy_calculated"], -0.5)

    def test_pipeline_writes_financial_and_release_panel(self) -> None:
        fake = FakeBaoStock()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provenance = run_pipeline(
                DEFAULT_COMPANIES_CONFIG,
                DEFAULT_RELEASES_CONFIG,
                root / "dataset",
                root / "raw",
                request_interval=0,
                baostock_module=fake,
            )
            self.assertTrue(fake.logged_out)
            self.assertEqual(provenance["financial_row_count"], 20)
            self.assertEqual(provenance["release_annotation_count"], 32)
            with (root / "dataset" / "annual_panel_with_release_notes.csv").open(
                encoding="utf-8", newline=""
            ) as handle:
                panel = list(csv.DictReader(handle))
            self.assertEqual(len(panel), 32)
            self.assertTrue(all(row["representative_release_titles"] for row in panel))
            tencent_2025 = next(
                row
                for row in panel
                if row["company_id"] == "tencent" and row["fiscal_year"] == "2025"
            )
            self.assertEqual(tencent_2025["representative_release_titles"], "VALORANT Mobile")
            self.assertEqual(
                tencent_2025["financial_data_status"],
                "official_filing_registered_not_parsed",
            )
            raw_lines = list((root / "raw" / "raw" / "baostock").rglob("responses.jsonl"))
            self.assertEqual(len(raw_lines), 1)
            first = json.loads(raw_lines[0].read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(first["endpoint"], "profit")


if __name__ == "__main__":
    unittest.main()
