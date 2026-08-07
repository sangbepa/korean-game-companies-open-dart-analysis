from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from build_lakehouse_metadata import build_lakehouse, target_raw_path


class LakehouseBootstrapTests(unittest.TestCase):
    def make_fixture(self, root: Path) -> tuple[Path, Path, Path, Path, Path]:
        legacy = root / "legacy"
        lake = root / "lake"
        config = root / "config"
        object_relative = Path(
            "bronze/company_ir/source=fixture_ir/ingest_date=2026-07-26/report.pdf"
        )
        object_path = legacy / object_relative
        object_path.parent.mkdir(parents=True)
        payload = b"%PDF-1.4\nfixture\n%%EOF\n"
        object_path.write_bytes(payload)
        sha256 = hashlib.sha256(payload).hexdigest()

        state = {
            "version": 1,
            "objects_by_sha256": {sha256: object_relative.as_posix()},
            "urls": {
                "https://example.test/report.pdf": {
                    "checksum_sha256": sha256,
                    "object_path": object_relative.as_posix(),
                    "last_seen_at": "2026-07-26T00:00:01Z",
                }
            },
        }
        state_path = legacy / "state" / "collector_state.json"
        state_path.parent.mkdir(parents=True)
        state_path.write_text(json.dumps(state), encoding="utf-8")

        manifest = {
            "manifest_version": 1,
            "run_id": "fixture_run",
            "started_at": "2026-07-26T00:00:00Z",
            "finished_at": "2026-07-26T00:00:02Z",
            "source_count": 2,
            "request_count": 2,
            "status_counts": {"fetched": 1, "error": 1},
            "results": [
                {
                    "source_id": "fixture_ir",
                    "company": "Fixture Games",
                    "category": "company_ir",
                    "url": "https://example.test/report.pdf",
                    "parent_url": None,
                    "retrieved_at": "2026-07-26T00:00:01Z",
                    "status": "fetched",
                    "http_status": 200,
                    "final_url": "https://example.test/report.pdf",
                    "content_type": "application/pdf",
                    "size_bytes": len(payload),
                    "checksum_sha256": sha256,
                    "object_path": object_relative.as_posix(),
                },
                {
                    "source_id": "fixture_old_source",
                    "company": "Fixture Games",
                    "category": "company_ir",
                    "url": "https://example.test/missing",
                    "parent_url": None,
                    "retrieved_at": "2026-07-26T00:00:02Z",
                    "status": "error",
                    "http_status": 404,
                    "error": "HTTP 404",
                },
            ],
        }
        manifest_path = legacy / "manifests" / "2026" / "07" / "26" / "run.json"
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        config.mkdir()
        companies = {
            "companies": [
                {
                    "company_id": "fixture",
                    "legal_name": "Fixture Games, Inc.",
                    "display_name": "Fixture Games",
                    "aliases": [],
                    "country_code": "KR",
                    "reporting_currency": "KRW",
                    "fiscal_year_end_month": 12,
                    "official_website": "https://example.test/",
                    "identifiers": [
                        {
                            "scheme": "ticker",
                            "value": "000001",
                            "market": "TEST",
                            "is_primary": True,
                            "source_url": "https://example.test/ir",
                        }
                    ],
                }
            ]
        }
        companies_path = config / "companies.json"
        companies_path.write_text(json.dumps(companies), encoding="utf-8")
        sources = {
            "sources": [
                {
                    "id": "fixture_ir",
                    "company": "Fixture Games",
                    "category": "company_ir",
                    "url": "https://example.test/ir",
                    "allowed_hosts": ["example.test"],
                }
            ]
        }
        sources_path = config / "sources.json"
        sources_path.write_text(json.dumps(sources), encoding="utf-8")
        return legacy, lake, companies_path, sources_path, object_path

    def scalar(self, parquet: Path, expression: str = "count(*)") -> object:
        connection = duckdb.connect(":memory:")
        try:
            return connection.execute(
                f"SELECT {expression} FROM read_parquet(?)", [str(parquet)]
            ).fetchone()[0]
        finally:
            connection.close()

    def test_bootstrap_is_non_destructive_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, legacy_object = self.make_fixture(root)
            original_mtime = legacy_object.stat().st_mtime_ns
            original_hash = hashlib.sha256(legacy_object.read_bytes()).hexdigest()

            first = build_lakehouse(legacy, lake, companies, sources, "copy")
            self.assertEqual(first["companies"], 1)
            self.assertEqual(first["sources"], 2)
            self.assertEqual(first["documents"], 1)
            self.assertEqual(first["ingestion_runs"], 1)
            self.assertEqual(first["ingestion_events"], 2)
            self.assertEqual(first["quality_issues"], 0)
            raw_files = list((lake / "raw" / "objects").rglob("*.*"))
            self.assertEqual(len(raw_files), 1)
            raw_mtime = raw_files[0].stat().st_mtime_ns
            self.assertEqual(hashlib.sha256(raw_files[0].read_bytes()).hexdigest(), original_hash)

            second = build_lakehouse(legacy, lake, companies, sources, "copy")
            self.assertEqual(second["documents"], 1)
            self.assertEqual(second["ingestion_events"], 2)
            self.assertEqual(len(list((lake / "raw" / "objects").rglob("*.*"))), 1)
            self.assertEqual(raw_files[0].stat().st_mtime_ns, raw_mtime)
            self.assertEqual(legacy_object.stat().st_mtime_ns, original_mtime)
            self.assertEqual(hashlib.sha256(legacy_object.read_bytes()).hexdigest(), original_hash)

            metadata = lake / "metadata"
            self.assertEqual(self.scalar(metadata / "company_master.parquet"), 1)
            self.assertEqual(self.scalar(metadata / "source_registry.parquet"), 2)
            self.assertEqual(self.scalar(metadata / "documents.parquet"), 1)
            self.assertEqual(self.scalar(metadata / "ingestion_runs.parquet"), 1)
            self.assertEqual(self.scalar(metadata / "ingestion_manifest.parquet"), 2)
            self.assertEqual(
                self.scalar(
                    metadata / "ingestion_manifest.parquet",
                    "count(*) FILTER (WHERE status='error' AND document_id IS NULL)",
                ),
                1,
            )

    def test_rejects_unsafe_legacy_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, _ = self.make_fixture(root)
            state_path = legacy / "state" / "collector_state.json"
            state = json.loads(state_path.read_text())
            sha256 = next(iter(state["objects_by_sha256"]))
            state["objects_by_sha256"][sha256] = "../outside.pdf"
            state_path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertRaises(ValueError):
                build_lakehouse(legacy, lake, companies, sources, "copy")

    def test_same_document_keeps_multiple_source_relationships(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, _ = self.make_fixture(root)
            manifest_path = next((legacy / "manifests").rglob("*.json"))
            manifest = json.loads(manifest_path.read_text())
            shared = dict(manifest["results"][0])
            shared["source_id"] = "fixture_old_source"
            shared["url"] = "https://example.test/shared-report.pdf"
            manifest["results"].append(shared)
            manifest["request_count"] = 3
            manifest["status_counts"] = {"fetched": 2, "error": 1}
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            summary = build_lakehouse(legacy, lake, companies, sources, "copy")
            self.assertEqual(summary["documents"], 1)
            self.assertEqual(summary["source_documents"], 2)
            self.assertEqual(summary["quality_issues"], 0)

    def test_missing_successful_object_is_logged_without_dangling_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, _ = self.make_fixture(root)
            state_path = legacy / "state" / "collector_state.json"
            state = json.loads(state_path.read_text())
            state["objects_by_sha256"] = {}
            state_path.write_text(json.dumps(state), encoding="utf-8")

            summary = build_lakehouse(legacy, lake, companies, sources, "copy")
            self.assertEqual(summary["documents"], 0)
            self.assertEqual(summary["quality_issues"], 1)
            metadata = lake / "metadata"
            self.assertEqual(
                self.scalar(
                    metadata / "ingestion_manifest.parquet",
                    "count(*) FILTER (WHERE status='fetched' AND document_id IS NULL)",
                ),
                1,
            )
            self.assertEqual(
                self.scalar(
                    metadata / "data_quality_log.parquet",
                    "count(*) FILTER (WHERE check_name='successful_event_document_exists')",
                ),
                1,
            )

    def test_rejects_unsafe_target_state_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, _ = self.make_fixture(root)
            state_path = lake / "metadata" / "collector_state.json"
            state_path.parent.mkdir(parents=True)
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "objects_by_sha256": {"0" * 64: "../outside.pdf"},
                        "urls": {},
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                build_lakehouse(legacy, lake, companies, sources, "copy")

    def test_refuses_conflicting_existing_raw_object(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, legacy_object = self.make_fixture(root)
            checksum = hashlib.sha256(legacy_object.read_bytes()).hexdigest()
            relative = target_raw_path(
                checksum,
                legacy_object.relative_to(legacy),
            )
            conflicting = lake / relative
            conflicting.parent.mkdir(parents=True)
            conflicting.write_bytes(b"conflicting bytes")
            with self.assertRaises(ValueError):
                build_lakehouse(legacy, lake, companies, sources, "copy")

    def test_manifest_company_id_and_global_regulatory_index_are_supported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, lake, companies, sources, _ = self.make_fixture(root)
            manifest_path = next((legacy / "manifests").rglob("*.json"))
            manifest = json.loads(manifest_path.read_text())
            manifest["results"].extend(
                [
                    {
                        "source_id": "fixture_edinet_xbrl",
                        "company": "Fixture Games",
                        "company_id": "fixture",
                        "category": "regulatory_filing",
                        "url": "https://api.example.test/documents/S100TEST?type=1",
                        "request_method": "GET",
                        "retrieved_at": "2026-07-26T00:00:03Z",
                        "status": "error",
                        "error": "fixture",
                    },
                    {
                        "source_id": "edinet_documents_list",
                        "company": "",
                        "company_id": None,
                        "category": "regulatory_index",
                        "url": "https://api.example.test/documents.json?date=2026-07-26&type=2",
                        "request_method": "GET",
                        "retrieved_at": "2026-07-26T00:00:04Z",
                        "status": "error",
                        "error": "fixture",
                    },
                ]
            )
            manifest["request_count"] = 4
            manifest["status_counts"] = {"fetched": 1, "error": 3}
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            summary = build_lakehouse(legacy, lake, companies, sources, "copy")
            self.assertEqual(summary["quality_issues"], 0)
            connection = duckdb.connect(":memory:")
            try:
                rows = connection.execute(
                    "SELECT source_id, company_id FROM read_parquet(?) "
                    "WHERE source_id IN ('fixture_edinet_xbrl', 'edinet_documents_list') "
                    "ORDER BY source_id",
                    [str(lake / "metadata" / "ingestion_manifest.parquet")],
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(
                rows,
                [("edinet_documents_list", None), ("fixture_edinet_xbrl", "fixture")],
            )


if __name__ == "__main__":
    unittest.main()
