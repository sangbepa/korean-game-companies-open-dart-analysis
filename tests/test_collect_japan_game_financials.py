from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
import urllib.parse
import zipfile
from pathlib import Path

from collect_japan_game_financials import (
    DEFAULT_COMPANIES_CONFIG,
    DEFAULT_RELEASES_CONFIG,
    add_calculated_growth,
    collect_edinet_metadata,
    download_edinet_xbrl_archives,
    extract_level_zero_items,
    load_json,
    resolve_edinet_api_key,
    run_pipeline,
    validate_configs,
)


def leaf(name: str, values: list[int]) -> dict[str, object]:
    return {
        "name": name,
        "unit": "MJPY",
        "ChildSeries": [],
        "Data": [{"nr": value, "str": str(value)} for value in values],
    }


def fake_euroland_payload() -> list[dict[str, object]]:
    years = ["FY2022", "FY2023", "FY2024", "FY2025"]
    return [
        {
            "name": "Custom AD",
            "Data": [
                {
                    "name": "Statement of Income",
                    "Columns": years,
                    "Series": [
                        leaf("Net sales", [100, 120, 150, 180]),
                        leaf("Gross profit", [60, 75, 90, 110]),
                        leaf("Operating profit", [40, 48, 60, 72]),
                        leaf("Profit before income taxes", [39, 47, 59, 71]),
                        leaf(
                            "Profit attributable to owners of the parent",
                            [30, 36, 45, 54],
                        ),
                    ],
                },
                {
                    "name": "Balance Sheet",
                    "Columns": years,
                    "Series": [
                        leaf("Total assets", [200, 220, 250, 300]),
                        leaf("Current assets", [160, 170, 190, 230]),
                        leaf("Non-current assets", [40, 50, 60, 70]),
                        leaf("Total net assets", [150, 165, 190, 230]),
                        leaf("Current liabilities", [40, 42, 45, 50]),
                        leaf("Non-current liabilities", [10, 13, 15, 20]),
                    ],
                },
                {
                    "name": "Statement of Cash Flows",
                    "Columns": years,
                    "Series": [
                        leaf("Cash flows from operating activities", [35, 40, 45, 50]),
                        leaf("Cash flows from investing activities", [-5, -6, -8, -10]),
                        leaf("Cash flows from financing activities", [-10, -12, -14, -16]),
                    ],
                },
                {
                    "name": "Business Segments",
                    "Columns": years,
                    "Series": [
                        {
                            "name": "Net sales",
                            "ChildSeries": [
                                leaf("Digital Contents", [80, 96, 120, 144])
                            ],
                        },
                        {
                            "name": "Operating income",
                            "ChildSeries": [
                                leaf("Digital Contents", [38, 46, 57, 68])
                            ],
                        },
                    ],
                },
            ],
        }
    ]


def fake_html() -> bytes:
    payload = json.dumps(fake_euroland_payload(), separators=(",", ":"))
    return f"<script>var Options={{,LevelZeroItems : {payload},levelZeroType:3}};</script>".encode()


class JapanGameCollectorTests(unittest.TestCase):
    def test_downloads_edinet_xbrl_zip_without_persisting_key(self) -> None:
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w") as archive:
            archive.writestr("XBRL/PublicDoc/test.xbrl", "<xbrl />")

        captured_urls: list[str] = []

        def fetcher(url: str) -> bytes:
            captured_urls.append(url)
            return archive_buffer.getvalue()

        with tempfile.TemporaryDirectory() as directory:
            manifest = download_edinet_xbrl_archives(
                [{"doc_id": "S100TEST", "xbrl_flag": "1"}],
                "secret-key",
                Path(directory),
                fetcher,
            )
            self.assertEqual(manifest[0]["status"], "collected")
            self.assertEqual(manifest[0]["xbrl_entry_count"], 1)
            self.assertTrue(captured_urls and "secret-key" in captured_urls[0])
            self.assertNotIn("secret-key", json.dumps(manifest))

    def test_reads_edinet_key_from_gitignored_local_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            key_file = Path(directory) / "edinet_api_key"
            key_file.write_text("local-secret-key\n", encoding="utf-8")
            self.assertEqual(
                resolve_edinet_api_key(key_file=key_file), "local-secret-key"
            )

    def test_repository_configs_cover_capcom_fiscal_years(self) -> None:
        companies = load_json(DEFAULT_COMPANIES_CONFIG)
        releases = load_json(DEFAULT_RELEASES_CONFIG)
        validate_configs(companies, releases)
        self.assertEqual(len(companies["companies"]), 1)
        self.assertEqual(companies["companies"][0]["edinet_code"], "E02417")
        self.assertEqual(len(releases["releases"]), 4)

    def test_extracts_embedded_euroland_json(self) -> None:
        payload = extract_level_zero_items(fake_html().decode())
        self.assertEqual(payload[0]["name"], "Custom AD")
        self.assertEqual(payload[0]["Data"][0]["Columns"][-1], "FY2025")

    def test_calculates_growth_without_losing_zero_values(self) -> None:
        rows = [
            {
                "company_id": "x",
                "fiscal_year": 2022,
                "net_sales_m_jpy": 100,
                "operating_profit_m_jpy": 20,
                "profit_attributable_to_owners_m_jpy": 10,
            },
            {
                "company_id": "x",
                "fiscal_year": 2023,
                "net_sales_m_jpy": 125,
                "operating_profit_m_jpy": 0,
                "profit_attributable_to_owners_m_jpy": 5,
            },
        ]
        add_calculated_growth(rows)
        self.assertAlmostEqual(rows[1]["revenue_yoy_calculated"], 0.25)
        self.assertAlmostEqual(rows[1]["operating_profit_yoy_calculated"], -1.0)
        self.assertAlmostEqual(rows[1]["net_profit_yoy_calculated"], -0.5)

    def test_pipeline_writes_capcom_financial_release_panel_and_raw_files(self) -> None:
        def fetcher(url: str) -> bytes:
            if "euroland.com" in url:
                return fake_html()
            if url.endswith(".pdf"):
                return b"%PDF-1.7 fake annual securities report"
            raise AssertionError(f"unexpected URL: {url}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provenance = run_pipeline(
                output_dir=root / "output",
                raw_root=root / "raw",
                fetcher=fetcher,
                request_interval=0,
            )
            self.assertEqual(provenance["financial_row_count"], 4)
            self.assertEqual(provenance["release_annotation_count"], 4)
            self.assertEqual(provenance["raw_document_count"], 5)
            self.assertEqual(provenance["raw_download_error_count"], 0)
            with (root / "output" / "annual_panel_with_release_notes.csv").open(
                encoding="utf-8"
            ) as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[0]["fiscal_year_end"], "2023-03-31")
            self.assertEqual(rows[0]["representative_release_titles"], "Monster Hunter Rise: Sunbreak")
            self.assertEqual(rows[2]["representative_release_titles"], "Monster Hunter Wilds")
            self.assertAlmostEqual(float(rows[1]["revenue_yoy_calculated"]), 0.2)
            self.assertAlmostEqual(float(rows[-1]["operating_margin"]), 0.4)

    def test_edinet_metadata_filters_to_annual_capcom_reports(self) -> None:
        companies = load_json(DEFAULT_COMPANIES_CONFIG)["companies"]

        def fetcher(url: str) -> bytes:
            query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
            filing_date = query["date"]
            results = []
            if filing_date == "2026-06-16":
                results = [
                    {
                        "docID": "S100TEST",
                        "edinetCode": "E02417",
                        "docTypeCode": "120",
                        "docDescription": "有価証券報告書",
                        "submitDateTime": "2026-06-16 10:00",
                        "periodStart": "2025-04-01",
                        "periodEnd": "2026-03-31",
                        "xbrlFlag": "1",
                        "pdfFlag": "1",
                    },
                    {
                        "docID": "S100OTHER",
                        "edinetCode": "E00000",
                        "docTypeCode": "120",
                    },
                ]
            return json.dumps({"metadata": {}, "results": results}).encode()

        rows, raw = collect_edinet_metadata(companies, "secret-not-retained", fetcher)
        self.assertEqual([row["doc_id"] for row in rows], ["S100TEST"])
        self.assertGreater(len(raw), 1)
        self.assertNotIn("secret-not-retained", json.dumps(raw))


if __name__ == "__main__":
    unittest.main()
