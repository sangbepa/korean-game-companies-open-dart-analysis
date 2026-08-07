from __future__ import annotations

import csv
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb

from build_edinet_financials import (
    AccountMatcher,
    ContextInfo,
    build_financial_silver,
    build_ttm_rows,
    parse_csv_archive,
    reported_period_from_contexts,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def archive(entries: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as handle:
        for name, payload in entries.items():
            handle.writestr(name, payload)
    return output.getvalue()


def csv_archive(rows: list[list[str]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter="\t", quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    writer.writerow(["要素ID", "項目名", "コンテキストID", "相対年度", "連結・個別", "期間・時点", "ユニットID", "単位", "値"])
    writer.writerows(rows)
    return archive({"XBRL_TO_CSV/report.csv": output.getvalue().encode("utf-16")})


def xbrl_archive(revenue: str, assets: str, segment_revenue: str) -> bytes:
    payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
 xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
 xmlns:jpcrp_cor="http://disclosure.edinet-fsa.go.jp/taxonomy/jpcrp/2026-01-01/jpcrp_cor"
 xmlns:test="http://example.test/company/2026" xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <xbrli:context id="CurrentYearDuration">
    <xbrli:entity><xbrli:identifier scheme="test">E02417</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2025-04-01</xbrli:startDate><xbrli:endDate>2026-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="Prior1YearDuration">
    <xbrli:entity><xbrli:identifier scheme="test">E02417</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2024-04-01</xbrli:startDate><xbrli:endDate>2025-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:context id="CurrentYearInstant">
    <xbrli:entity><xbrli:identifier scheme="test">E02417</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2026-03-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:context id="CurrentYearDuration_testDigitalContentsMember">
    <xbrli:entity><xbrli:identifier scheme="test">E02417</xbrli:identifier>
      <xbrli:segment><xbrldi:explicitMember dimension="test:BusinessAxis">test:DigitalContentsMember</xbrldi:explicitMember></xbrli:segment>
    </xbrli:entity>
    <xbrli:period><xbrli:startDate>2025-04-01</xbrli:startDate><xbrli:endDate>2026-03-31</xbrli:endDate></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="JPY"><xbrli:measure>iso4217:JPY</xbrli:measure></xbrli:unit>
  <jpcrp_cor:NetSales contextRef="CurrentYearDuration" unitRef="JPY" decimals="-6">{revenue}</jpcrp_cor:NetSales>
  <jpcrp_cor:NetSales contextRef="Prior1YearDuration" unitRef="JPY" decimals="-6">900</jpcrp_cor:NetSales>
  <jpcrp_cor:Assets contextRef="CurrentYearInstant" unitRef="JPY" decimals="-6">{assets}</jpcrp_cor:Assets>
  <jpcrp_cor:RevenueFromExternalCustomers contextRef="CurrentYearDuration_testDigitalContentsMember" unitRef="JPY" decimals="-6">{segment_revenue}</jpcrp_cor:RevenueFromExternalCustomers>
  <jpcrp_cor:OperatingIncome contextRef="CurrentYearDuration" unitRef="JPY" xsi:nil="true"/>
</xbrli:xbrl>""".encode()
    return archive({"XBRL/PublicDoc/report.xbrl": payload})


class EdinetFinancialBuildTests(unittest.TestCase):
    def store_raw(self, lake: Path, payload: bytes, filename: str) -> tuple[str, str]:
        checksum = hashlib.sha256(payload).hexdigest()
        relative = Path("raw") / "objects" / f"sha256={checksum[:2]}" / f"{checksum}__{filename}"
        path = lake / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return checksum, relative.as_posix()

    def make_manifest(self, lake: Path, filings: list[dict[str, object]]) -> None:
        path = lake / "metadata" / "manifests_json" / "2026" / "06" / "20" / "fixture.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "manifest_version": 1,
            "pipeline_name": "edinet_financial_collection",
            "run_id": "fixture_collection",
            "started_at": "2026-06-20T01:00:00Z",
            "finished_at": "2026-06-20T01:01:00Z",
            "source_count": 1,
            "request_count": 2,
            "status_counts": {"fetched": 2},
            "results": [],
            "filings": filings,
        }), encoding="utf-8")

    def filing(self, doc_id: str, xbrl_id: str, xbrl_path: str, csv_id: str, csv_path: str, revenue: int = 1000) -> dict[str, object]:
        return {
            "company_id": "capcom",
            "company_name": "Capcom",
            "ticker": "9697",
            "edinet_code": "E02417",
            "doc_id": doc_id,
            "doc_type_code": "120",
            "report_type": "annual",
            "fiscal_year": 2026,
            "period_start": "2025-04-01",
            "period_end": "2026-03-31",
            "submitted_at": "2026-06-20T10:00:00+09:00",
            "filer_name": "CAPCOM CO., LTD.",
            "parent_doc_id": None,
            "is_amendment": False,
            "legal_status": "1",
            "accounting_standard_expected": "J-GAAP",
            "xbrl_document_id": xbrl_id,
            "csv_document_id": csv_id,
            "xbrl_raw_path": xbrl_path,
            "csv_raw_path": csv_path,
            "collection_status": "succeeded",
            "fixture_revenue": revenue,
        }

    def test_builds_financial_and_segment_facts_from_utf16_csv_and_xbrl(self) -> None:
        rows = [
            ["jpcrp_cor:NetSales", "売上高", "CurrentYearDuration", "CurrentYear", "連結", "期間", "JPY", "JPY", "1000"],
            ["jpcrp_cor:NetSales", "売上高", "Prior1YearDuration", "Prior1Year", "連結", "期間", "JPY", "JPY", "900"],
            ["jpcrp_cor:Assets", "資産合計", "CurrentYearInstant", "CurrentYear", "連結", "時点", "JPY", "JPY", "5000"],
            ["jpcrp_cor:RevenueFromExternalCustomers", "外部顧客への売上高", "CurrentYearDuration_testDigitalContentsMember", "CurrentYear", "連結", "期間", "JPY", "JPY", "700"],
            ["jpcrp_cor:OperatingIncome", "営業利益", "CurrentYearDuration", "CurrentYear", "連結", "期間", "JPY", "JPY", ""],
        ]
        with tempfile.TemporaryDirectory() as directory:
            lake = Path(directory) / "lake"
            xbrl_id, xbrl_path = self.store_raw(lake, xbrl_archive("1000", "5000", "700"), "report_xbrl.zip")
            csv_id, csv_path = self.store_raw(lake, csv_archive(rows), "report_csv.zip")
            self.make_manifest(lake, [self.filing("S100AAAA", xbrl_id, xbrl_path, csv_id, csv_path)])

            summary = build_financial_silver(
                lake,
                PROJECT_ROOT / "config" / "companies.json",
                PROJECT_ROOT / "config" / "edinet_accounts.json",
            )
            self.assertEqual(summary["status"], "succeeded")
            self.assertTrue(summary["published_to_current"])
            current = (lake / "silver" / "financial" / "CURRENT").resolve()
            connection = duckdb.connect(":memory:")
            try:
                facts = connection.execute(
                    "SELECT account_id, numeric_value, is_comparative, instant_date, decimals "
                    "FROM read_parquet(?) ORDER BY account_id, is_comparative",
                    [str(current / "financial_facts.parquet")],
                ).fetchall()
                segments = connection.execute(
                    "SELECT segment_id, metric_account_id, numeric_value FROM read_parquet(?)",
                    [str(current / "segment_facts.parquet")],
                ).fetchall()
            finally:
                connection.close()
            self.assertIn(("revenue", Decimal("1000.0000"), False, None, "-6"), facts)
            self.assertIn(("revenue", Decimal("900.0000"), True, None, "-6"), facts)
            self.assertIn(("total_assets", Decimal("5000.0000"), False, date(2026, 3, 31), "-6"), facts)
            self.assertEqual(segments, [("digital_contents", "segment_revenue", Decimal("700.0000"))])
            gold = (lake / "gold" / "edinet" / "CURRENT").resolve()
            self.assertTrue((gold / "company_ttm_metrics.parquet").is_file())

    def test_amendment_is_latest_effective(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lake = Path(directory) / "lake"
            filings = []
            for doc_id, submitted, revenue, amendment in (
                ("S100BASE", "2026-06-20T10:00:00+09:00", "1000", False),
                ("S100AMND", "2026-07-01T10:00:00+09:00", "1100", True),
            ):
                rows = [["jpcrp_cor:NetSales", "売上高", "CurrentYearDuration", "CurrentYear", "連結", "期間", "JPY", "JPY", revenue]]
                xbrl_id, xbrl_path = self.store_raw(lake, xbrl_archive(revenue, "5000", "700"), f"{doc_id}_xbrl.zip")
                csv_id, csv_path = self.store_raw(lake, csv_archive(rows), f"{doc_id}_csv.zip")
                filing = self.filing(doc_id, xbrl_id, xbrl_path, csv_id, csv_path)
                filing["submitted_at"] = submitted
                filing["is_amendment"] = amendment
                filing["doc_type_code"] = "130" if amendment else "120"
                filing["parent_doc_id"] = "S100BASE" if amendment else None
                filings.append(filing)
            self.make_manifest(lake, filings)
            summary = build_financial_silver(lake, PROJECT_ROOT / "config" / "companies.json", PROJECT_ROOT / "config" / "edinet_accounts.json")
            current = (lake / summary["snapshot_path"]).resolve()
            connection = duckdb.connect(":memory:")
            try:
                effective = connection.execute(
                    "SELECT filing_id FROM read_parquet(?) WHERE is_latest_effective",
                    [str(current / "filings.parquet")],
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(effective, [("S100AMND",)])

    def test_ttm_formula_and_missing_prior_half(self) -> None:
        filings = [
            {"filing_id": "annual", "company_id": "capcom", "report_type": "annual", "period_end": date(2025, 3, 31), "is_latest_effective": True},
            {"filing_id": "prior_half", "company_id": "capcom", "report_type": "semiannual", "period_end": date(2024, 9, 30), "is_latest_effective": True},
            {"filing_id": "current_half", "company_id": "capcom", "report_type": "semiannual", "period_end": date(2025, 9, 30), "is_latest_effective": True},
        ]
        facts = [
            {"filing_id": "annual", "account_id": "revenue", "numeric_value": Decimal("1000"), "is_comparative": False, "mapping_priority": 10, "source_concept": "Revenue", "context_id": "CurrentYear"},
            {"filing_id": "prior_half", "account_id": "revenue", "numeric_value": Decimal("450"), "is_comparative": False, "mapping_priority": 10, "source_concept": "Revenue", "context_id": "Interim"},
            {"filing_id": "current_half", "account_id": "revenue", "numeric_value": Decimal("600"), "is_comparative": False, "mapping_priority": 10, "source_concept": "Revenue", "context_id": "Interim"},
        ]
        rows, _, _ = build_ttm_rows(filings, facts, [], ["capcom"])
        revenue = next(row for row in rows if row[2] == "revenue")
        self.assertEqual(revenue[5], Decimal("1150"))
        self.assertEqual(revenue[-1], "complete")

        rows, _, _ = build_ttm_rows([row for row in filings if row["filing_id"] != "prior_half"], facts, [], ["capcom"])
        revenue = next(row for row in rows if row[2] == "revenue")
        self.assertIsNone(revenue[5])
        self.assertEqual(revenue[-1], "missing_prior_half")

    def test_zip_traversal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unsafe.zip"
            path.write_bytes(archive({"../escape.csv": b"bad"}))
            with self.assertRaises(ValueError):
                parse_csv_archive(path)

    def test_semiannual_period_comes_from_primary_xbrl_context(self) -> None:
        contexts = {
            "InterimDuration": ContextInfo(
                "InterimDuration", date(2025, 4, 1), date(2025, 9, 30), None, ()
            ),
            "CurrentYearDuration_DigitalMember": ContextInfo(
                "CurrentYearDuration_DigitalMember",
                date(2025, 4, 1),
                date(2025, 9, 30),
                None,
                ("example:DigitalMember",),
            ),
        }
        self.assertEqual(
            reported_period_from_contexts(contexts, "semiannual", "2025-04-01"),
            (date(2025, 4, 1), date(2025, 9, 30)),
        )

    def test_ifrs_concept_mapping(self) -> None:
        matcher = AccountMatcher(PROJECT_ROOT / "config" / "edinet_accounts.json")
        self.assertEqual(matcher.match("ifrs-full:Revenue", "Revenue").account_id, "revenue")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("ifrs-full:ProfitLossAttributableToOwnersOfParent", "",).account_id, "net_income_parent")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("capcom:CompanyExtensionAmount", "売上高").account_id, "revenue")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("jpigp_cor:OperatingProfitLossIFRS", "").account_id, "operating_income")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("jpigp_cor:CashAndCashEquivalentsIFRS", "").account_id, "cash_and_cash_equivalents")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("jpigp_cor:NetCashProvidedByUsedInOperatingActivitiesIFRS", "").account_id, "operating_cash_flow")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("sony:NetSalesIFRS", "").account_id, "revenue")  # type: ignore[union-attr]
        self.assertEqual(matcher.match("konami:NetSalesAndOperatingRevenueIFRS", "").account_id, "revenue")  # type: ignore[union-attr]

    def test_concept_mapping_rejects_substring_false_positives(self) -> None:
        matcher = AccountMatcher(PROJECT_ROOT / "config" / "edinet_accounts.json")
        self.assertIsNone(matcher.match("jppfs_cor:NonOperatingIncome", "営業外収益"))
        self.assertIsNone(
            matcher.match(
                "jppfs_cor:DecreaseIncreaseInInventoriesOpeCF",
                "棚卸資産の増減額（△は増加）、営業活動によるキャッシュ・フロー",
            )
        )
        self.assertEqual(
            matcher.match(
                "jppfs_cor:NetCashProvidedByUsedInOperatingActivities",
                "営業活動によるキャッシュ・フロー",
            ).account_id,  # type: ignore[union-attr]
            "operating_cash_flow",
        )
        self.assertEqual(
            matcher.match(
                "jppfs_cor:PurchaseOfPropertyPlantAndEquipmentAndIntangibleAssetsInvCF",
                "有形及び無形固定資産の取得による支出、投資活動によるキャッシュ・フロー",
            ).account_id,  # type: ignore[union-attr]
            "capital_expenditure",
        )
        self.assertEqual(
            matcher.match(
                "jpcrp_cor:ResearchAndDevelopmentExpensesIncludedInGeneralAndAdministrativeExpensesAndManufacturingCostForCurrentPeriod",
                "一般管理費及び当期製造費用に含まれる研究開発費",
            ).account_id,  # type: ignore[union-attr]
            "research_and_development_expense",
        )


if __name__ == "__main__":
    unittest.main()
