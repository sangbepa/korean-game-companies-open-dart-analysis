from __future__ import annotations

import json
import hashlib
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from collect_data_lake import Collector, Source, discover_document_urls, load_sources


class FixtureHandler(BaseHTTPRequestHandler):
    index = b'<html><body><a href="/report.pdf">result</a></body></html>'
    report = b"%PDF-1.4\nfixture\n%%EOF\n"

    def do_GET(self) -> None:
        if self.headers.get("If-None-Match") == '"fixture-v1"':
            self.send_response(304)
            self.end_headers()
            return
        if self.path == "/index.html":
            payload, content_type = self.index, "text/html; charset=utf-8"
        elif self.path == "/report.pdf":
            payload, content_type = self.report, "application/pdf"
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("ETag", '"fixture-v1"')
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        pass


class CollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def make_source(self) -> Source:
        port = self.server.server_address[1]
        return Source(
            id="fixture_source",
            company="Fixture Games",
            category="company_ir",
            url=f"http://127.0.0.1:{port}/index.html",
            allowed_hosts=("127.0.0.1",),
            max_documents=5,
        )

    def test_discovers_only_allowed_documents(self) -> None:
        source = self.make_source()
        html = b"""
        <a href="/a.pdf">A</a>
        <a href="https://outside.example/b.pdf">B</a>
        <a href="/ordinary-page">C</a>
        """
        self.assertEqual(
            discover_document_urls(html, source.url, source),
            [source.url.replace("index.html", "a.pdf")],
        )

    def test_end_to_end_collection_is_idempotent(self) -> None:
        source = self.make_source()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "lake"
            config_path = Path(directory) / "sources.json"
            config_path.write_text('{"sources": []}', encoding="utf-8")

            first = Collector(root, min_interval_seconds=0, retries=0).run(
                [source], config_path
            )
            self.assertEqual(first["status_counts"], {"fetched": 2})
            objects = list((root / "raw").rglob("*.*"))
            self.assertEqual(len(objects), 2)

            second = Collector(root, min_interval_seconds=0, retries=0).run(
                [source], config_path
            )
            self.assertEqual(second["status_counts"], {"not_modified": 1})
            self.assertEqual(len(list((root / "raw").rglob("*.*"))), 2)
            manifests = root / "metadata" / "manifests_json"
            self.assertEqual(len(list(manifests.rglob("*.json"))), 2)

    def test_discovers_document_url_embedded_in_json(self) -> None:
        source = self.make_source()
        payload = b'{"report":"http://127.0.0.1/report.pdf"}'
        self.assertEqual(
            discover_document_urls(payload, source.url, source),
            ["http://127.0.0.1/report.pdf"],
        )

    def test_expands_file_ids_from_json(self) -> None:
        source = Source(
            **{
                **self.make_source().__dict__,
                "json_id_fields": ("englishFileId",),
                "json_url_template": "http://127.0.0.1/api/files/{value}/download",
                "link_allow_patterns": (r"(?i)/api/files/.+/download",),
            }
        )
        payload = b'{"rows":[{"englishFileId":42},{"englishFileId":0}]}'
        self.assertEqual(
            discover_document_urls(payload, source.url, source),
            ["http://127.0.0.1/api/files/42/download"],
        )

    def test_config_rejects_seed_host_outside_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "sources.json"
            config.write_text(
                json.dumps(
                    {
                        "sources": [
                            {
                                "id": "bad_source",
                                "company": "Bad",
                                "category": "company_ir",
                                "url": "https://example.com/",
                                "allowed_hosts": ["other.example"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                load_sources(config)

    def test_rejects_unsafe_object_path_in_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "lake"
            state_path = root / "metadata" / "collector_state.json"
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
                Collector(root, min_interval_seconds=0, retries=0)

    def test_invalid_304_cache_is_refetched_without_conditions(self) -> None:
        source = self.make_source()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "lake"
            config_path = Path(directory) / "sources.json"
            config_path.write_text('{"sources": []}', encoding="utf-8")
            checksum = hashlib.sha256(FixtureHandler.index).hexdigest()
            missing_path = (
                Path("raw")
                / "objects"
                / f"sha256={checksum[:2]}"
                / f"{checksum}__missing.html"
            )
            state_path = root / "metadata" / "collector_state.json"
            state_path.parent.mkdir(parents=True)
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "objects_by_sha256": {checksum: missing_path.as_posix()},
                        "urls": {
                            f"GET {source.url}": {
                                "checksum_sha256": checksum,
                                "object_path": missing_path.as_posix(),
                                "etag": '"fixture-v1"',
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            manifest = Collector(root, min_interval_seconds=0, retries=0).run(
                [source], config_path
            )
            self.assertEqual(manifest["status_counts"], {"unchanged": 1, "fetched": 1})
            self.assertEqual(len(list((root / "raw" / "objects").rglob("*.*"))), 2)


if __name__ == "__main__":
    unittest.main()
