#!/usr/bin/env python3
"""Collect disclosure metadata and optional original documents from Open DART.

This is deliberately a small, storage-agnostic collector.  It writes a merged
JSON Lines catalog, immutable original ZIP files, raw API page responses, and a
manifest for each run.  A database or data-lake layout can be added later.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
import sys
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_COMPANIES = PROJECT_ROOT / "config" / "companies.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "dart_data"
DEFAULT_API_BASE = "https://opendart.fss.or.kr/api"
DEFAULT_API_KEY_ENV = "OPENDART_API_KEY"
DEFAULT_USER_AGENT = "game-accounting-dart-collector/0.1"
TRANSIENT_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}
VALID_DISCLOSURE_TYPES = set("ABCDEFGHIJ")


class DartError(RuntimeError):
    """An Open DART or local collection error safe to show to the user."""


@dataclass(frozen=True)
class Company:
    company_id: str
    display_name: str
    corp_code: str
    stock_code: str | None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_json_write(path: Path, payload: Any) -> None:
    rendered = json.dumps(
        payload, ensure_ascii=False, indent=2, sort_keys=True
    ).encode("utf-8") + b"\n"
    atomic_write(path, rendered)


def atomic_jsonl_write(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rendered = b"".join(
        (
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
        ).encode("utf-8")
        for row in rows
    )
    atomic_write(path, rendered)


def load_companies(path: Path) -> list[Company]:
    """Load only companies that have an eight-digit DART corporation code."""
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    raw_companies = payload.get("companies")
    if not isinstance(raw_companies, list):
        raise ValueError("Company config must contain a 'companies' list")

    companies: list[Company] = []
    seen_ids: set[str] = set()
    seen_codes: set[str] = set()
    for raw in raw_companies:
        identifiers = raw.get("identifiers", [])
        corp_codes = [
            str(item.get("value", ""))
            for item in identifiers
            if item.get("scheme") == "dart_corp_code"
        ]
        if not corp_codes:
            continue
        if len(corp_codes) != 1 or not (
            len(corp_codes[0]) == 8 and corp_codes[0].isdigit()
        ):
            raise ValueError(
                f"Invalid DART corporation code for {raw.get('company_id')!r}"
            )
        company_id = str(raw.get("company_id", ""))
        display_name = str(raw.get("display_name", ""))
        if not company_id or not display_name:
            raise ValueError("Every DART company needs company_id and display_name")
        if company_id in seen_ids or corp_codes[0] in seen_codes:
            raise ValueError(f"Duplicate DART company mapping for {company_id!r}")

        stock_codes = [
            str(item.get("value", ""))
            for item in identifiers
            if item.get("scheme") == "ticker"
            and str(item.get("market", "")).startswith("KRX")
        ]
        companies.append(
            Company(
                company_id=company_id,
                display_name=display_name,
                corp_code=corp_codes[0],
                stock_code=stock_codes[0] if stock_codes else None,
            )
        )
        seen_ids.add(company_id)
        seen_codes.add(corp_codes[0])
    return companies


def validate_date(value: str, name: str) -> str:
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as error:
        raise ValueError(f"{name} must be a real date in YYYYMMDD format") from error
    return value


def extract_api_error(payload: bytes) -> tuple[str, str] | None:
    """Read a DART status response returned as JSON or XML."""
    try:
        parsed = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        parsed = None
    if isinstance(parsed, dict) and parsed.get("status"):
        return str(parsed["status"]), str(parsed.get("message", ""))

    text = payload.decode("utf-8", errors="replace")
    import re

    status_match = re.search(r"<status>\s*([^<]+)\s*</status>", text)
    message_match = re.search(r"<message>\s*([^<]*)\s*</message>", text)
    if status_match:
        return (
            status_match.group(1).strip(),
            message_match.group(1).strip() if message_match else "",
        )
    return None


def parse_list_payload(payload: bytes) -> dict[str, Any]:
    try:
        result = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DartError("Open DART returned a non-JSON disclosure response") from error
    if not isinstance(result, dict):
        raise DartError("Open DART disclosure response is not an object")
    status = str(result.get("status", ""))
    if status not in {"000", "013"}:
        raise DartError(
            f"Open DART error {status or 'unknown'}: {result.get('message', '')}"
        )
    return result


class RateLimiter:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(0.0, interval_seconds)
        self.last_request_at: float | None = None

    def wait(self) -> None:
        if self.last_request_at is not None:
            remaining = self.interval_seconds - (time.monotonic() - self.last_request_at)
            if remaining > 0:
                time.sleep(remaining)
        self.last_request_at = time.monotonic()


class DartClient:
    """Small Open DART HTTP client that never exposes its API key."""

    def __init__(
        self,
        api_key: str,
        *,
        api_base: str = DEFAULT_API_BASE,
        timeout_seconds: float = 30.0,
        max_bytes: int = 100 * 1024 * 1024,
        min_interval_seconds: float = 0.25,
        retries: int = 2,
        user_agent: str = DEFAULT_USER_AGENT,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        if len(api_key) != 40:
            raise ValueError("Open DART API key must contain exactly 40 characters")
        self._api_key = api_key
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_bytes = max_bytes
        self.retries = max(0, retries)
        self.user_agent = user_agent
        self.rate_limiter = RateLimiter(min_interval_seconds)
        self._opener = opener

    def _read_limited(self, response: Any) -> bytes:
        declared = response.headers.get("Content-Length")
        if declared:
            try:
                declared_size = int(declared)
            except ValueError as error:
                raise DartError("Open DART returned an invalid Content-Length") from error
            if declared_size > self.max_bytes:
                raise DartError(
                    f"Open DART response exceeds {self.max_bytes} byte limit"
                )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = response.read(min(1024 * 1024, self.max_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > self.max_bytes:
                raise DartError(
                    f"Open DART response exceeds {self.max_bytes} byte limit"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def request(self, endpoint: str, parameters: dict[str, str]) -> bytes:
        query = urlencode({"crtfc_key": self._api_key, **parameters})
        url = f"{self.api_base}/{endpoint}?{query}"
        request = Request(
            url,
            headers={
                "Accept-Encoding": "identity",
                "User-Agent": self.user_agent,
            },
        )
        for attempt in range(self.retries + 1):
            self.rate_limiter.wait()
            try:
                response = self._opener(request, timeout=self.timeout_seconds)
                try:
                    return self._read_limited(response)
                finally:
                    response.close()
            except HTTPError as error:
                code = error.code
                error.close()
                if code not in TRANSIENT_HTTP_CODES or attempt >= self.retries:
                    raise DartError(
                        f"Open DART HTTP {code} while requesting {endpoint}"
                    ) from error
            except URLError as error:
                if attempt >= self.retries:
                    raise DartError(
                        f"Open DART network error while requesting {endpoint}: "
                        f"{error.reason}"
                    ) from error
            if attempt < self.retries:
                time.sleep(2**attempt)
        raise AssertionError("unreachable")

    def search_disclosures(
        self,
        company: Company,
        start_date: str,
        end_date: str,
        *,
        last_only: bool,
        disclosure_type: str | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        filings: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        page_no = 1
        while True:
            parameters = {
                "corp_code": company.corp_code,
                "bgn_de": start_date,
                "end_de": end_date,
                "last_reprt_at": "Y" if last_only else "N",
                "sort": "date",
                "sort_mth": "asc",
                "page_no": str(page_no),
                "page_count": "100",
            }
            if disclosure_type:
                parameters["pblntf_ty"] = disclosure_type
            page = parse_list_payload(self.request("list.json", parameters))
            pages.append(page)
            if str(page.get("status")) == "013":
                break
            raw_list = page.get("list", [])
            if not isinstance(raw_list, list):
                raise DartError("Open DART disclosure list is not an array")
            filings.extend(item for item in raw_list if isinstance(item, dict))
            total_pages = int(page.get("total_page", 1))
            if page_no >= total_pages:
                break
            page_no += 1
        return filings, pages

    def download_original(self, receipt_number: str) -> bytes:
        payload = self.request("document.xml", {"rcept_no": receipt_number})
        if not zipfile.is_zipfile(io.BytesIO(payload)):
            api_error = extract_api_error(payload)
            if api_error:
                raise DartError(
                    f"Open DART error {api_error[0]} for {receipt_number}: "
                    f"{api_error[1]}"
                )
            raise DartError(
                f"Open DART original for {receipt_number} is not a valid ZIP file"
            )
        return payload


def load_catalog(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {path} at line {line_number}"
                ) from error
            receipt_number = str(row.get("rcept_no", ""))
            if len(receipt_number) != 14 or not receipt_number.isdigit():
                raise ValueError(
                    f"Invalid receipt number in {path} at line {line_number}"
                )
            rows[receipt_number] = row
    return rows


class DartCollector:
    def __init__(self, output_root: Path, client: DartClient) -> None:
        self.output_root = output_root.resolve()
        self.client = client
        self.catalog_path = self.output_root / "disclosures.jsonl"

    def _original_path(self, company: Company, filing: dict[str, Any]) -> Path:
        receipt_number = str(filing.get("rcept_no", ""))
        receipt_date = str(filing.get("rcept_dt", ""))
        if len(receipt_number) != 14 or not receipt_number.isdigit():
            raise DartError(f"Invalid DART receipt number: {receipt_number!r}")
        if len(receipt_date) != 8 or not receipt_date.isdigit():
            raise DartError(f"Invalid DART receipt date: {receipt_date!r}")
        return (
            self.output_root
            / "originals"
            / company.corp_code
            / receipt_date[:4]
            / f"{receipt_number}.zip"
        )

    def _collect_original(
        self, company: Company, filing: dict[str, Any]
    ) -> tuple[str, str, str]:
        destination = self._original_path(company, filing)
        relative = destination.relative_to(self.output_root).as_posix()
        if destination.exists():
            if not destination.is_file() or not zipfile.is_zipfile(destination):
                raise DartError(f"Existing original is not a valid ZIP: {relative}")
            checksum = hashlib.sha256(destination.read_bytes()).hexdigest()
            return "existing", relative, checksum

        receipt_number = str(filing["rcept_no"])
        payload = self.client.download_original(receipt_number)
        checksum = hashlib.sha256(payload).hexdigest()
        atomic_write(destination, payload)
        return "downloaded", relative, checksum

    def run(
        self,
        companies: list[Company],
        *,
        start_date: str,
        end_date: str,
        last_only: bool,
        disclosure_type: str | None,
        download_originals: bool,
        company_config_path: Path,
    ) -> dict[str, Any]:
        self.output_root.mkdir(parents=True, exist_ok=True)
        lock_path = self.output_root / ".collector.lock"
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                return self._run_locked(
                    companies,
                    start_date=start_date,
                    end_date=end_date,
                    last_only=last_only,
                    disclosure_type=disclosure_type,
                    download_originals=download_originals,
                    company_config_path=company_config_path,
                )
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _run_locked(
        self,
        companies: list[Company],
        *,
        start_date: str,
        end_date: str,
        last_only: bool,
        disclosure_type: str | None,
        download_originals: bool,
        company_config_path: Path,
    ) -> dict[str, Any]:
        started = utc_now()
        observed_at = isoformat(started)
        run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        catalog = load_catalog(self.catalog_path)
        filings_seen = 0
        downloaded = 0
        existing = 0
        errors: list[dict[str, str]] = []
        company_counts: dict[str, int] = {}

        for company in companies:
            try:
                filings, pages = self.client.search_disclosures(
                    company,
                    start_date,
                    end_date,
                    last_only=last_only,
                    disclosure_type=disclosure_type,
                )
            except DartError as error:
                errors.append({"company_id": company.company_id, "error": str(error)})
                continue

            company_counts[company.company_id] = len(filings)
            filings_seen += len(filings)
            response_dir = self.output_root / "api_responses" / run_id / company.company_id
            for page_number, page in enumerate(pages, start=1):
                atomic_json_write(response_dir / f"page_{page_number:05d}.json", page)

            for filing in filings:
                receipt_number = str(filing.get("rcept_no", ""))
                if len(receipt_number) != 14 or not receipt_number.isdigit():
                    errors.append(
                        {
                            "company_id": company.company_id,
                            "error": f"Invalid receipt number: {receipt_number!r}",
                        }
                    )
                    continue
                previous = catalog.get(receipt_number, {})
                row = {
                    **previous,
                    **filing,
                    "company_id": company.company_id,
                    "configured_company_name": company.display_name,
                    "configured_corp_code": company.corp_code,
                    "first_seen_at": previous.get("first_seen_at", observed_at),
                    "last_seen_at": observed_at,
                    "viewer_url": (
                        "https://dart.fss.or.kr/dsaf001/main.do?rcpNo="
                        f"{receipt_number}"
                    ),
                }
                if download_originals:
                    try:
                        status, relative, checksum = self._collect_original(
                            company, filing
                        )
                        row["original_status"] = status
                        row["original_path"] = relative
                        row["original_sha256"] = checksum
                        if status == "downloaded":
                            downloaded += 1
                        else:
                            existing += 1
                    except DartError as error:
                        row["original_status"] = "error"
                        errors.append(
                            {
                                "company_id": company.company_id,
                                "rcept_no": receipt_number,
                                "error": str(error),
                            }
                        )
                else:
                    row.setdefault("original_status", "not_requested")
                catalog[receipt_number] = row

        ordered_rows = sorted(
            catalog.values(),
            key=lambda row: (str(row.get("rcept_dt", "")), str(row.get("rcept_no", ""))),
        )
        atomic_jsonl_write(self.catalog_path, ordered_rows)
        finished = utc_now()
        manifest = {
            "manifest_version": 1,
            "run_id": run_id,
            "status": "complete" if not errors else "partial",
            "started_at": isoformat(started),
            "finished_at": isoformat(finished),
            "company_config_path": str(company_config_path.resolve()),
            "output_root": str(self.output_root),
            "query": {
                "start_date": start_date,
                "end_date": end_date,
                "last_only": last_only,
                "disclosure_type": disclosure_type,
                "download_originals": download_originals,
            },
            "company_count": len(companies),
            "company_filing_counts": company_counts,
            "filings_seen": filings_seen,
            "catalog_rows": len(ordered_rows),
            "originals_downloaded": downloaded,
            "originals_existing": existing,
            "error_count": len(errors),
            "errors": errors,
        }
        manifest_path = self.output_root / "runs" / f"{run_id}.json"
        atomic_json_write(manifest_path, manifest)
        manifest["manifest_path"] = str(manifest_path)
        return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    today = date.today()
    parser = argparse.ArgumentParser(
        description="Collect Open DART disclosure metadata and optional originals."
    )
    parser.add_argument("--companies", type=Path, default=DEFAULT_COMPANIES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--company",
        action="append",
        default=[],
        help="Collect one configured company_id; may be repeated.",
    )
    parser.add_argument("--list-companies", action="store_true")
    parser.add_argument("--start-date", default=f"{today.year}0101")
    parser.add_argument("--end-date", default=today.strftime("%Y%m%d"))
    parser.add_argument(
        "--last-only",
        action="store_true",
        help="Ask DART for final reports only; default keeps corrections too.",
    )
    parser.add_argument(
        "--disclosure-type",
        choices=sorted(VALID_DISCLOSURE_TYPES),
        help="Optional DART type A-J. Omit to collect all disclosure types.",
    )
    parser.add_argument(
        "--download-originals",
        action="store_true",
        help="Also download each filing's original XML ZIP.",
    )
    parser.add_argument("--api-key-env", default=DEFAULT_API_KEY_ENV)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-megabytes", type=int, default=100)
    parser.add_argument("--min-interval", type=float, default=0.25)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        companies = load_companies(args.companies)
        if args.list_companies:
            for company in companies:
                print(
                    f"{company.company_id:16} {company.corp_code} "
                    f"{company.stock_code or '-':6} {company.display_name}"
                )
            return 0

        if args.company:
            requested = set(args.company)
            known = {company.company_id for company in companies}
            unknown = sorted(requested - known)
            if unknown:
                raise ValueError(f"Unknown company ids: {', '.join(unknown)}")
            companies = [
                company for company in companies if company.company_id in requested
            ]
        if not companies:
            raise ValueError("No companies with DART corporation codes were selected")

        start_date = validate_date(args.start_date, "--start-date")
        end_date = validate_date(args.end_date, "--end-date")
        if start_date > end_date:
            raise ValueError("--start-date must not be later than --end-date")

        api_key = os.environ.get(args.api_key_env, "")
        if not api_key:
            raise ValueError(
                f"Set the Open DART API key in the {args.api_key_env} environment variable"
            )
        client = DartClient(
            api_key,
            api_base=args.api_base,
            timeout_seconds=args.timeout,
            max_bytes=args.max_megabytes * 1024 * 1024,
            min_interval_seconds=args.min_interval,
            retries=args.retries,
            user_agent=args.user_agent,
        )
        manifest = DartCollector(args.output, client).run(
            companies,
            start_date=start_date,
            end_date=end_date,
            last_only=args.last_only,
            disclosure_type=args.disclosure_type,
            download_originals=args.download_originals,
            company_config_path=args.companies,
        )
        print(f"Run: {manifest['run_id']}")
        print(f"Status: {manifest['status']}")
        print(f"Companies: {manifest['company_count']}")
        print(f"Filings seen: {manifest['filings_seen']}")
        print(f"Catalog rows: {manifest['catalog_rows']}")
        print(f"Originals downloaded: {manifest['originals_downloaded']}")
        print(f"Originals already present: {manifest['originals_existing']}")
        print(f"Errors: {manifest['error_count']}")
        print(f"Manifest: {manifest['manifest_path']}")
        return 0 if not manifest["errors"] else 1
    except (DartError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
