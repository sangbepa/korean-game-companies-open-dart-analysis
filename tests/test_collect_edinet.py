from __future__ import annotations

import io
import json
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

from collect_edinet import (
    EdinetCollector,
    EdinetError,
    classify_filing,
    load_edinet_companies,
    select_filings,
    validate_edinet_code_list,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def zip_payload(name: str, payload: bytes) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(name, payload)
    return output.getvalue()


class FakeEdinetCollector(EdinetCollector):
    def __init__(self, lake_root: Path, responses: dict[tuple[str, tuple[tuple[str, str], ...]], bytes]) -> None:
        super().__init__(lake_root, "super-secret-key", min_interval_seconds=0, retries=0)
        self.responses = responses
        self.calls: list[tuple[str, dict[str, str]]] = []

    def _request_bytes(self, path: str, params: dict[str, str]):  # type: ignore[override]
        self.calls.append((path, dict(params)))
        key = (path, tuple(sorted(params.items())))
        if key not in self.responses:
            raise EdinetError(f"missing fixture response: {key}")
        data = self.responses[key]
        content_type = "application/json" if path.endswith(".json") else "application/zip"
        return 200, {"Content-Type": content_type}, data


class EdinetCollectorTests(unittest.TestCase):
    def test_company_config_has_the_twelve_verified_targets(self) -> None:
        companies = load_edinet_companies(PROJECT_ROOT / "config" / "companies.json")
        self.assertEqual(len(companies), 12)
        self.assertEqual({company.ticker for company in companies}, {
            "3659", "7974", "9697", "9684", "3635", "9766",
            "6460", "7832", "6758", "4751", "3765", "3668",
        })

    def test_official_code_list_pair_validation(self) -> None:
        companies = load_edinet_companies(PROJECT_ROOT / "config" / "companies.json")
        lines = ["header"] + [
            f'"{company.edinet_code}","issuer","{company.ticker}0","corporate"'
            for company in companies
        ]
        validate_edinet_code_list("\r\n".join(lines).encode("cp932"), companies)
        with self.assertRaises(ValueError):
            validate_edinet_code_list(b'"E00000","issuer","00000",', companies)

    def test_quarterly_filter_accepts_only_legacy_second_quarter(self) -> None:
        q1 = {
            "docTypeCode": "140",
            "docDescription": "First Quarter Report",
            "periodStart": "2023-04-01",
            "periodEnd": "2023-06-30",
        }
        q2 = {
            **q1,
            "docDescription": "Second Quarter Report",
            "periodEnd": "2023-09-30",
        }
        self.assertIsNone(classify_filing(q1))
        self.assertEqual(classify_filing(q2), "semiannual")

    def test_selection_keeps_configured_company_and_five_fiscal_years(self) -> None:
        companies = load_edinet_companies(PROJECT_ROOT / "config" / "companies.json")
        capcom = next(company for company in companies if company.company_id == "capcom")
        rows = []
        for year in range(2020, 2027):
            rows.append({
                "docID": f"S{year}ABC1",
                "edinetCode": capcom.edinet_code,
                "secCode": capcom.ticker + "0",
                "docTypeCode": "120",
                "periodStart": f"{year - 1}-04-01",
                "periodEnd": f"{year}-03-31",
                "legalStatus": "1",
            })
        selected = select_filings(rows, [capcom], 5)
        self.assertEqual({row["fiscal_year"] for row in selected}, {2022, 2023, 2024, 2025, 2026})

    def test_append_only_run_redacts_key_and_reuses_document_objects(self) -> None:
        companies = load_edinet_companies(PROJECT_ROOT / "config" / "companies.json")
        capcom = next(company for company in companies if company.company_id == "capcom")
        listing = json.dumps({
            "metadata": {"status": "200"},
            "results": [{
                "docID": "S100ABCD",
                "edinetCode": capcom.edinet_code,
                "secCode": capcom.ticker + "0",
                "filerName": capcom.display_name,
                "docTypeCode": "120",
                "docDescription": "Annual Securities Report",
                "periodStart": "2025-04-01",
                "periodEnd": "2026-03-31",
                "submitDateTime": "2026-06-20T10:00:00+09:00",
                "xbrlFlag": "1",
                "csvFlag": "1",
                "legalStatus": "1",
            }],
        }).encode()
        archive = zip_payload("PublicDoc/report.xbrl", b"<xbrl/>")
        responses = {
            ("documents.json", (("date", "2026-06-20"), ("type", "2"))): listing,
            ("documents/S100ABCD", (("type", "1"),)): archive,
            ("documents/S100ABCD", (("type", "5"),)): zip_payload("XBRL_TO_CSV/report.csv", b"fixture"),
        }
        with tempfile.TemporaryDirectory() as directory:
            lake = Path(directory) / "lake"
            collector = FakeEdinetCollector(lake, responses)
            first = collector.run([capcom], date(2026, 6, 20), date(2026, 6, 20))
            self.assertEqual(len(first["filings"]), 1)
            self.assertEqual(first["filings"][0]["collection_status"], "succeeded")
            manifest_text = Path(first["manifest_path"]).read_text()
            self.assertNotIn("super-secret-key", manifest_text)
            self.assertNotIn("Subscription-Key", manifest_text)
            self.assertEqual(len(list((lake / "raw" / "objects").rglob("*.*"))), 3)

            second = collector.run([capcom], date(2026, 6, 20), date(2026, 6, 20))
            self.assertEqual(len(list((lake / "raw" / "objects").rglob("*.*"))), 3)
            self.assertEqual(len(list((lake / "metadata" / "manifests_json").rglob("*.json"))), 2)
            self.assertEqual(second["filings"][0]["xbrl_status"], "unchanged")
            self.assertEqual(
                json.loads((lake / "metadata" / "collector_state.json").read_text())["edinet"]["last_successful_list_date"],
                "2026-06-20",
            )

    def test_http_200_error_json_is_rejected_as_document(self) -> None:
        with self.assertRaises(EdinetError):
            EdinetCollector.validate_zip_response(
                b'{"metadata":{"status":"404","message":"not found"}}',
                "S100ABCD",
                "xbrl",
            )

    def test_missing_api_key_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            EdinetCollector(Path("/tmp/unused"), "")

    def test_transient_network_error_is_retried(self) -> None:
        class Response:
            status = 200
            headers = {"Content-Type": "application/json", "Content-Length": "2"}

            def __enter__(self):
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, size: int = -1) -> bytes:
                return b"{}"

        collector = EdinetCollector(Path("/tmp/unused"), "secret", retries=1, min_interval_seconds=0)
        with patch("collect_edinet.urlopen", side_effect=[URLError("temporary"), Response()]) as mocked:
            status, _, payload = collector._request_bytes("documents.json", {"date": "2026-01-01", "type": "2"})
        self.assertEqual(status, 200)
        self.assertEqual(payload, b"{}")
        self.assertEqual(mocked.call_count, 2)


if __name__ == "__main__":
    unittest.main()
