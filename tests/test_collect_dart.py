from __future__ import annotations

import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from collect_dart import (
    Company,
    DartCollector,
    DartError,
    extract_api_error,
    load_catalog,
    load_companies,
    parse_list_payload,
    validate_date,
)


def zip_payload(name: str = "report.xml", body: bytes = b"<DOCUMENT />") -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(name, body)
    return output.getvalue()


class FakeClient:
    def __init__(self) -> None:
        self.download_calls: list[str] = []

    def search_disclosures(self, company, start_date, end_date, **kwargs):
        filing = {
            "corp_cls": "K",
            "corp_name": "테스트게임즈",
            "corp_code": company.corp_code,
            "stock_code": company.stock_code,
            "report_nm": "사업보고서 (2025.12)",
            "rcept_no": "20260331000001",
            "flr_nm": "테스트게임즈",
            "rcept_dt": "20260331",
            "rm": "연",
        }
        page = {
            "status": "000",
            "message": "정상",
            "page_no": 1,
            "page_count": 100,
            "total_count": 1,
            "total_page": 1,
            "list": [filing],
        }
        return [filing], [page]

    def download_original(self, receipt_number):
        self.download_calls.append(receipt_number)
        return zip_payload()


class DartCollectorTests(unittest.TestCase):
    def test_company_config_extracts_eight_dart_companies(self) -> None:
        config = Path(__file__).resolve().parents[1] / "config" / "companies.json"
        companies = load_companies(config)
        self.assertEqual(len(companies), 8)
        self.assertNotIn("nexon", {company.company_id for company in companies})
        self.assertEqual(companies[0].corp_code, "00261443")

    def test_date_validation_rejects_impossible_dates(self) -> None:
        self.assertEqual(validate_date("20260726", "date"), "20260726")
        with self.assertRaises(ValueError):
            validate_date("20260230", "date")

    def test_list_payload_accepts_no_data_and_rejects_api_errors(self) -> None:
        no_data = parse_list_payload(
            json.dumps({"status": "013", "message": "조회된 데이타가 없습니다."}).encode()
        )
        self.assertEqual(no_data["status"], "013")
        with self.assertRaises(DartError):
            parse_list_payload(
                json.dumps({"status": "020", "message": "요청 제한"}).encode()
            )

    def test_extracts_xml_error_from_document_endpoint(self) -> None:
        self.assertEqual(
            extract_api_error(
                b'<?xml version="1.0"?><result><status>014</status>'
                b"<message>missing</message></result>"
            ),
            ("014", "missing"),
        )

    def test_collection_merges_catalog_and_skips_existing_original(self) -> None:
        company = Company("test_games", "Test Games", "00123456", "123456")
        fake_client = FakeClient()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "dart"
            config = Path(directory) / "companies.json"
            config.write_text('{"companies": []}', encoding="utf-8")
            collector = DartCollector(output, fake_client)
            arguments = {
                "start_date": "20260101",
                "end_date": "20260726",
                "last_only": False,
                "disclosure_type": None,
                "download_originals": True,
                "company_config_path": config,
            }

            first = collector.run([company], **arguments)
            second = collector.run([company], **arguments)

            self.assertEqual(first["originals_downloaded"], 1)
            self.assertEqual(second["originals_existing"], 1)
            self.assertEqual(fake_client.download_calls, ["20260331000001"])
            catalog = load_catalog(output / "disclosures.jsonl")
            self.assertEqual(len(catalog), 1)
            row = catalog["20260331000001"]
            self.assertEqual(row["company_id"], "test_games")
            self.assertEqual(row["original_status"], "existing")
            self.assertTrue((output / row["original_path"]).is_file())
            self.assertNotIn("crtfc_key", first)


if __name__ == "__main__":
    unittest.main()
