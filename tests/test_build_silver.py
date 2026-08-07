from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

from build_lakehouse_metadata import write_table
from build_silver import (
    AccountMatcher,
    SilverBuilder,
    build_silver,
    scalar_fields,
    select_worker_python,
    utc_now,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SilverBuildTests(unittest.TestCase):
    def test_zero_is_preserved_as_numeric_value(self) -> None:
        self.assertEqual(scalar_fields(0)["numeric_value"], "0")

    def make_lake(self, root: Path) -> tuple[Path, list[Path]]:
        lake = root / "lake"
        metadata = lake / "metadata"
        metadata.mkdir(parents=True)
        payloads = [
            (
                "page.html",
                b"<html><body><p>Revenue 100</p><table><tr><th>Metric</th>"
                b"<th>2025</th></tr><tr><td>Revenue</td><td>100</td></tr>"
                b"</table><a href='https://example.test/report.pdf'>Report</a></body></html>",
            ),
            (
                "index.json",
                b'{"reports":[{"url":"https://example.test/report.pdf","amount":123}]}',
            ),
        ]
        raw_paths: list[Path] = []
        document_rows = []
        relation_rows = []
        for index, (name, payload) in enumerate(payloads, start=1):
            checksum = hashlib.sha256(payload).hexdigest()
            relative = (
                Path("raw")
                / "objects"
                / f"sha256={checksum[:2]}"
                / f"{checksum}__{name}"
            )
            raw = lake / relative
            raw.parent.mkdir(parents=True, exist_ok=True)
            raw.write_bytes(payload)
            raw_paths.append(raw)
            extension = Path(name).suffix
            document_rows.append(
                (
                    checksum,
                    checksum,
                    len(payload),
                    extension,
                    "source_snapshot",
                    relative.as_posix(),
                    "valid",
                )
            )
            relation_rows.append(
                (
                    checksum,
                    f"fixture_{index}",
                    "fixture",
                    2025,
                    f"https://example.test/{name}",
                )
            )

        connection = duckdb.connect(":memory:")
        try:
            write_table(
                connection,
                "documents",
                (
                    ("document_id", "VARCHAR"),
                    ("sha256", "VARCHAR"),
                    ("size_bytes", "BIGINT"),
                    ("file_extension", "VARCHAR"),
                    ("document_kind", "VARCHAR"),
                    ("raw_path", "VARCHAR"),
                    ("integrity_status", "VARCHAR"),
                ),
                document_rows,
                metadata / "documents.parquet",
            )
            write_table(
                connection,
                "source_documents",
                (
                    ("document_id", "VARCHAR"),
                    ("source_id", "VARCHAR"),
                    ("company_id", "VARCHAR"),
                    ("report_year", "INTEGER"),
                    ("source_url", "VARCHAR"),
                ),
                relation_rows,
                metadata / "source_documents.parquet",
            )
        finally:
            connection.close()
        return lake, raw_paths

    def scalar(self, path: Path, expression: str = "count(*)") -> object:
        connection = duckdb.connect(":memory:")
        try:
            return connection.execute(
                f"SELECT {expression} FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
        finally:
            connection.close()

    def test_html_json_build_is_complete_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake, raw_paths = self.make_lake(root)
            before = [(path.stat().st_mtime_ns, path.read_bytes()) for path in raw_paths]
            arguments = (
                lake,
                PROJECT_ROOT / "config" / "account_aliases.json",
                PROJECT_ROOT / "extract_document_worker.py",
                None,
            )

            first = build_silver(*arguments)
            self.assertEqual(first["status"], "succeeded")
            self.assertEqual(first["documents"], 2)
            self.assertEqual(first["succeeded"], 2)
            self.assertEqual(first["failed"], 0)
            self.assertGreaterEqual(first["tables"], 1)
            self.assertGreaterEqual(first["table_cells"], 4)
            self.assertGreaterEqual(first["structured_values"], 2)
            self.assertGreaterEqual(first["discovered_links"], 2)
            self.assertGreaterEqual(first["reported_line_candidates"], 1)

            second = build_silver(*arguments)
            self.assertEqual(first["dataset_hashes"]["content_units"], second["dataset_hashes"]["content_units"])
            self.assertEqual(first["dataset_hashes"]["tables"], second["dataset_hashes"]["tables"])
            self.assertEqual(first["dataset_hashes"]["table_cells"], second["dataset_hashes"]["table_cells"])
            self.assertEqual(
                [(path.stat().st_mtime_ns, path.read_bytes()) for path in raw_paths],
                before,
            )

            silver = lake / "silver"
            self.assertTrue((silver / "CURRENT").is_symlink())
            snapshots = sorted((silver / "snapshots").iterdir())
            self.assertEqual(len(snapshots), 2)
            active = (silver / "CURRENT").resolve()
            self.assertEqual(self.scalar(active / "extraction_runs.parquet"), 2)
            self.assertEqual(self.scalar(active / "document_extractions.parquet"), 4)
            self.assertEqual(
                self.scalar(
                    active / "reported_line_candidates.parquet",
                    "count(*) FILTER (WHERE review_status <> 'unreviewed')",
                ),
                0,
            )
            connection = duckdb.connect(":memory:")
            try:
                columns = {
                    row[0]
                    for row in connection.execute(
                        "DESCRIBE SELECT * FROM read_parquet(?)",
                        [str(active / "reported_line_candidates.parquet")],
                    ).fetchall()
                }
            finally:
                connection.close()
            self.assertNotIn("company_id", columns)
            self.assertNotIn("fiscal_period", columns)

    def test_rejects_raw_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake, _ = self.make_lake(root)
            documents = lake / "metadata" / "documents.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute("CREATE TABLE docs AS SELECT * FROM read_parquet(?)", [str(documents)])
                connection.execute("UPDATE docs SET raw_path='../outside.html'")
                connection.execute("COPY docs TO ? (FORMAT PARQUET)", [str(documents)])
            finally:
                connection.close()
            with self.assertRaises(ValueError):
                build_silver(
                    lake,
                    PROJECT_ROOT / "config" / "account_aliases.json",
                    PROJECT_ROOT / "extract_document_worker.py",
                    None,
                )

    def test_rejects_symlink_escape_from_raw_objects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake, _ = self.make_lake(root)
            escape = lake / "raw" / "objects" / "metadata_link"
            escape.symlink_to(lake / "metadata", target_is_directory=True)
            documents = lake / "metadata" / "documents.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    "CREATE TABLE docs AS SELECT * FROM read_parquet(?)", [str(documents)]
                )
                connection.execute(
                    "UPDATE docs SET raw_path='raw/objects/metadata_link/documents.parquet'"
                )
                connection.execute("COPY docs TO ? (FORMAT PARQUET)", [str(documents)])
            finally:
                connection.close()
            with self.assertRaises(ValueError):
                build_silver(
                    lake,
                    PROJECT_ROOT / "config" / "account_aliases.json",
                    PROJECT_ROOT / "extract_document_worker.py",
                    None,
                )

    def test_failed_document_rolls_back_partial_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lake = Path(directory) / "lake"
            payload = b"<html><body>Revenue 100</body></html>"
            checksum = hashlib.sha256(payload).hexdigest()
            relative = (
                Path("raw")
                / "objects"
                / f"sha256={checksum[:2]}"
                / f"{checksum}__fixture.html"
            )
            raw = lake / relative
            raw.parent.mkdir(parents=True)
            raw.write_bytes(payload)
            matcher = AccountMatcher(PROJECT_ROOT / "config" / "account_aliases.json")
            builder = SilverBuilder(
                lake,
                matcher,
                PROJECT_ROOT / "extract_document_worker.py",
                Path(sys.executable),
                "fixture-runtime",
                "fixture-run",
                utc_now(),
            )

            def fail_after_emitting(**kwargs: object) -> dict[str, object]:
                builder.add_content_unit(
                    extraction_id=str(kwargs["extraction_id"]),
                    document_id=str(kwargs["document_id"]),
                    unit_kind="html_document",
                    ordinal=1,
                    locator={"dom": "document"},
                    raw_text="Revenue 100",
                    extraction_method="fixture",
                )
                raise RuntimeError("fixture failure")

            builder.extract_html = fail_after_emitting  # type: ignore[method-assign]
            builder.extract_document(
                {
                    "document_id": checksum,
                    "sha256": checksum,
                    "size_bytes": len(payload),
                    "file_extension": ".html",
                    "document_kind": "source_snapshot",
                    "raw_path": relative.as_posix(),
                    "integrity_status": "valid",
                    "relations": [],
                }
            )
            self.assertEqual(builder.extractions[0]["status"], "failed")
            self.assertEqual(builder.content_units, [])
            self.assertEqual(builder.tables, [])
            self.assertEqual(builder.table_cells, [])
            self.assertEqual(builder.line_candidates, [])
            self.assertEqual(len(builder.quality), 1)

    def test_failed_run_keeps_current_and_history_is_merged_later(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lake, _ = self.make_lake(root)
            arguments = (
                lake,
                PROJECT_ROOT / "config" / "account_aliases.json",
                PROJECT_ROOT / "extract_document_worker.py",
                None,
            )
            first = build_silver(*arguments)
            self.assertTrue(first["published_to_current"])
            current = lake / "silver" / "CURRENT"
            first_snapshot = current.resolve()

            documents = lake / "metadata" / "documents.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    "CREATE TABLE docs AS SELECT * FROM read_parquet(?)", [str(documents)]
                )
                connection.execute("UPDATE docs SET integrity_status='invalid'")
                connection.execute("COPY docs TO ? (FORMAT PARQUET)", [str(documents)])
            finally:
                connection.close()

            failed = build_silver(*arguments)
            self.assertEqual(failed["status"], "failed")
            self.assertFalse(failed["published_to_current"])
            self.assertEqual(current.resolve(), first_snapshot)
            diagnostic = lake / failed["snapshot_path"]
            self.assertTrue(diagnostic.is_dir())

            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    "CREATE TABLE docs AS SELECT * FROM read_parquet(?)", [str(documents)]
                )
                connection.execute("UPDATE docs SET integrity_status='valid'")
                connection.execute("COPY docs TO ? (FORMAT PARQUET)", [str(documents)])
            finally:
                connection.close()

            recovered = build_silver(*arguments)
            self.assertEqual(recovered["status"], "succeeded")
            self.assertTrue(recovered["published_to_current"])
            active = current.resolve()
            self.assertNotEqual(active, first_snapshot)
            self.assertEqual(self.scalar(active / "extraction_runs.parquet"), 3)
            self.assertEqual(self.scalar(active / "document_extractions.parquet"), 6)

    def test_xlsx_worker_preserves_formula_merge_and_hidden_column(self) -> None:
        try:
            worker_python = select_worker_python(None)
        except ValueError as error:
            self.skipTest(str(error))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workbook = root / "fixture.xlsx"
            creator = subprocess.run(
                [
                    str(worker_python),
                    "-c",
                    (
                        "import openpyxl,sys;"
                        "w=openpyxl.Workbook();s=w.active;s.title='Consolidated IS';"
                        "s['A1']='Revenue';s['B1']='2025';s['A2']='Revenue';"
                        "s['B2']='=[1]Other!A1';s['A3']='Merged';s.merge_cells('A3:B3');"
                        "s.column_dimensions['B'].hidden=True;w.save(sys.argv[1])"
                    ),
                    str(workbook),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(creator.returncode, 0, creator.stderr)
            original_hash = hashlib.sha256(workbook.read_bytes()).hexdigest()
            original_mtime = workbook.stat().st_mtime_ns
            process = subprocess.run(
                [
                    str(worker_python),
                    str(PROJECT_ROOT / "extract_document_worker.py"),
                    "--path",
                    str(workbook),
                    "--format",
                    "xlsx",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
            records = [json.loads(line) for line in process.stdout.splitlines() if line]
            formula = next(
                row
                for row in records
                if row.get("record_type") == "cell" and row.get("cell_reference") == "B2"
            )
            sheet = next(row for row in records if row.get("record_type") == "sheet")
            self.assertTrue(formula["formula_is_external"])
            self.assertEqual(formula["formula"], "=[1]Other!A1")
            self.assertEqual(sheet["hidden_column_count"], 1)
            self.assertIn("A3:B3", sheet["merged_ranges_json"])
            self.assertEqual(hashlib.sha256(workbook.read_bytes()).hexdigest(), original_hash)
            self.assertEqual(workbook.stat().st_mtime_ns, original_mtime)


if __name__ == "__main__":
    unittest.main()
