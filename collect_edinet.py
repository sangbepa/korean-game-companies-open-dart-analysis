#!/usr/bin/env python3
"""Collect Japanese game-company filings from the official EDINET API v2.

The collector shares the lakehouse content-addressed Raw object store and
generic ingestion manifest format. API keys are added only to the outbound
request and are never persisted in state, URLs, manifests, or error messages.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sys
import time
import uuid
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from collect_data_lake import atomic_json_write, validated_object_path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_COMPANIES = PROJECT_ROOT / "config" / "companies.json"
DEFAULT_LAKE_ROOT = PROJECT_ROOT / "game_accounting_lake"
DEFAULT_API_BASE = "https://api.edinet-fsa.go.jp/api/v2"
DEFAULT_API_KEY_ENV = "EDINET_API_KEY"
TARGET_DOC_TYPES = {"120", "130", "140", "150", "160", "170"}
ANNUAL_DOC_TYPES = {"120", "130"}
HALF_DOC_TYPES = {"160", "170"}
LEGACY_QUARTER_DOC_TYPES = {"140", "150"}
CORRECTION_DOC_TYPES = {"130", "150", "170"}
SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9_]{1,63}$")
DOC_ID_PATTERN = re.compile(r"^S[0-9A-Z]{7,11}$")
EDINET_CODE_PATTERN = re.compile(r"^E\d{5}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class EdinetError(RuntimeError):
    """Raised for an invalid EDINET response or configuration."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def date_range(start: date, end: date) -> Iterable[date]:
    if start > end:
        raise ValueError("start date must not be after end date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


@dataclass(frozen=True)
class EdinetCompany:
    company_id: str
    display_name: str
    ticker: str
    edinet_code: str
    fiscal_year_end_month: int
    accounting_standard: str


@dataclass
class FetchRecord:
    source_id: str
    company: str
    company_id: str | None
    category: str
    url: str
    request_method: str
    parent_url: str | None
    retrieved_at: str
    status: str
    http_status: int | None = None
    final_url: str | None = None
    content_type: str | None = None
    size_bytes: int | None = None
    checksum_sha256: str | None = None
    object_path: str | None = None
    error: str | None = None


def _identifier(company: dict[str, Any], scheme: str) -> dict[str, Any] | None:
    matches = [
        item
        for item in company.get("identifiers", [])
        if str(item.get("scheme")) == scheme
    ]
    if len(matches) > 1 and scheme == "edinet_code":
        raise ValueError(f"Multiple EDINET codes for {company.get('company_id')}")
    if scheme == "ticker":
        matches = [
            item for item in matches if str(item.get("market", "")).startswith("TSE")
        ]
    return matches[0] if matches else None


def load_edinet_companies(path: Path) -> list[EdinetCompany]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    raw_companies = payload.get("companies")
    if not isinstance(raw_companies, list):
        raise ValueError("Company config must contain a companies list")
    companies: list[EdinetCompany] = []
    seen_tickers: set[str] = set()
    seen_edinet: set[str] = set()
    for raw in raw_companies:
        ticker_item = _identifier(raw, "ticker")
        edinet_item = _identifier(raw, "edinet_code")
        if str(raw.get("country_code")) != "JP" or not edinet_item:
            continue
        if not ticker_item:
            raise ValueError(f"Japanese EDINET company lacks a TSE ticker: {raw}")
        company_id = str(raw.get("company_id") or "")
        ticker = str(ticker_item.get("value") or "")
        edinet_code = str(edinet_item.get("value") or "")
        if not SAFE_ID.fullmatch(company_id):
            raise ValueError(f"Unsafe EDINET company id: {company_id!r}")
        if not re.fullmatch(r"\d{4}", ticker):
            raise ValueError(f"Invalid TSE ticker for {company_id}: {ticker!r}")
        if not EDINET_CODE_PATTERN.fullmatch(edinet_code):
            raise ValueError(f"Invalid EDINET code for {company_id}: {edinet_code!r}")
        if ticker in seen_tickers or edinet_code in seen_edinet:
            raise ValueError(f"Duplicate Japanese company identifier for {company_id}")
        seen_tickers.add(ticker)
        seen_edinet.add(edinet_code)
        companies.append(
            EdinetCompany(
                company_id=company_id,
                display_name=str(raw["display_name"]),
                ticker=ticker,
                edinet_code=edinet_code,
                fiscal_year_end_month=int(raw["fiscal_year_end_month"]),
                accounting_standard=str(raw.get("accounting_standard") or "unknown"),
            )
        )
    if not companies:
        raise ValueError("No Japanese companies with EDINET codes are configured")
    return companies


def validate_edinet_code_list(payload: bytes, companies: list[EdinetCompany]) -> None:
    """Validate configured ticker/EDINET pairs against an official code-list CSV."""
    text: str | None = None
    for encoding in ("cp932", "utf-8-sig", "utf-16"):
        try:
            text = payload.decode(encoding)
            break
        except UnicodeError:
            continue
    if text is None:
        raise ValueError("EDINET code list uses an unsupported encoding")
    pairs = set(re.findall(r'"(E\d{5})"[^\r\n]*,"(\d{5})"(?:,|\r?$)', text, re.MULTILINE))
    missing = [
        company.company_id
        for company in companies
        if (company.edinet_code, company.ticker + "0") not in pairs
    ]
    if missing:
        raise ValueError("EDINET code-list mismatch: " + ", ".join(sorted(missing)))


def read_edinet_code_list(path: Path) -> bytes:
    if path.suffix.casefold() != ".zip":
        return path.read_bytes()
    try:
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if Path(name).name.casefold() == "edinetcodedlinfo.csv"]
            if len(names) != 1:
                raise ValueError("EDINET code-list ZIP must contain EdinetcodeDlInfo.csv")
            return archive.read(names[0])
    except zipfile.BadZipFile as error:
        raise ValueError("Invalid EDINET code-list ZIP") from error


def fiscal_year(period_end: date | None, fiscal_end_month: int) -> int | None:
    if period_end is None:
        return None
    return period_end.year + (1 if period_end.month > fiscal_end_month else 0)


def is_legacy_half_year(item: dict[str, Any]) -> bool:
    description = str(item.get("docDescription") or "")
    if re.search(r"第[２2]四半期|second\s+quarter", description, re.IGNORECASE):
        return True
    start = parse_iso_date(item.get("periodStart"))
    end = parse_iso_date(item.get("periodEnd"))
    if not start or not end:
        return False
    return 145 <= (end - start).days <= 215


def classify_filing(item: dict[str, Any]) -> str | None:
    doc_type = str(item.get("docTypeCode") or "")
    if doc_type in ANNUAL_DOC_TYPES:
        return "annual"
    if doc_type in HALF_DOC_TYPES:
        return "semiannual"
    if doc_type in LEGACY_QUARTER_DOC_TYPES and is_legacy_half_year(item):
        return "semiannual"
    return None


def select_filings(
    items: list[dict[str, Any]],
    companies: list[EdinetCompany],
    history_years: int,
) -> list[dict[str, Any]]:
    by_edinet = {company.edinet_code: company for company in companies}
    candidates: list[dict[str, Any]] = []
    for item in items:
        company = by_edinet.get(str(item.get("edinetCode") or ""))
        if not company:
            continue
        sec_code = str(item.get("secCode") or "")
        if sec_code and not sec_code.startswith(company.ticker):
            continue
        report_type = classify_filing(item)
        if not report_type or str(item.get("docTypeCode") or "") not in TARGET_DOC_TYPES:
            continue
        if str(item.get("legalStatus") or "1") == "0":
            continue
        doc_id = str(item.get("docID") or "")
        if not DOC_ID_PATTERN.fullmatch(doc_id):
            continue
        period_end = parse_iso_date(item.get("periodEnd"))
        rendered = dict(item)
        rendered["company_id"] = company.company_id
        rendered["company_name"] = company.display_name
        rendered["ticker"] = company.ticker
        rendered["report_type"] = report_type
        rendered["fiscal_year"] = fiscal_year(period_end, company.fiscal_year_end_month)
        rendered["accounting_standard_expected"] = company.accounting_standard
        candidates.append(rendered)

    selected: list[dict[str, Any]] = []
    for company in companies:
        rows = [row for row in candidates if row["company_id"] == company.company_id]
        annual_years = sorted(
            {int(row["fiscal_year"]) for row in rows if row["report_type"] == "annual" and row.get("fiscal_year")},
            reverse=True,
        )
        allowed = set(annual_years[:history_years])
        if allowed:
            allowed.add(max(allowed) + 1)
            rows = [row for row in rows if row.get("fiscal_year") in allowed]
        selected.extend(rows)
    return sorted(
        selected,
        key=lambda row: (
            row["company_id"],
            str(row.get("periodEnd") or ""),
            str(row.get("submitDateTime") or ""),
            row["docID"],
        ),
    )


class EdinetCollector:
    def __init__(
        self,
        lake_root: Path,
        api_key: str,
        *,
        api_base: str = DEFAULT_API_BASE,
        timeout_seconds: float = 30.0,
        retries: int = 3,
        min_interval_seconds: float = 0.25,
        max_bytes: int = 250 * 1024 * 1024,
        user_agent: str = "game-accounting-edinet/0.1",
    ) -> None:
        if not api_key.strip():
            raise ValueError("EDINET API key is required")
        self.lake_root = lake_root.resolve()
        self.api_key = api_key.strip()
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.min_interval_seconds = max(0.0, min_interval_seconds)
        self.max_bytes = max_bytes
        self.user_agent = user_agent
        self.state_path = self.lake_root / "metadata" / "collector_state.json"
        self.state: dict[str, Any] = {}
        self._last_request = 0.0

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"version": 1, "objects_by_sha256": {}, "urls": {}, "edinet": {}}
        with self.state_path.open(encoding="utf-8") as handle:
            state = json.load(handle)
        if state.get("version") != 1:
            raise ValueError("Unsupported collector state version")
        state.setdefault("objects_by_sha256", {})
        state.setdefault("urls", {})
        state.setdefault("edinet", {})
        if not all(isinstance(state[key], dict) for key in ("objects_by_sha256", "urls", "edinet")):
            raise ValueError("Collector state indexes must be mappings")
        for checksum, relative in state["objects_by_sha256"].items():
            if not SHA256_PATTERN.fullmatch(str(checksum)):
                raise ValueError(f"Invalid checksum in collector state: {checksum!r}")
            validated_object_path(self.lake_root, str(relative))
        return state

    def default_dates(self, today: date, history_years: int) -> tuple[date, date]:
        self.state = self._load_state()
        watermark = parse_iso_date(self.state["edinet"].get("last_successful_list_date"))
        if watermark:
            return max(date(today.year - history_years - 1, 1, 1), watermark - timedelta(days=6)), today
        return date(today.year - history_years - 1, 1, 1), today

    def _wait(self) -> None:
        remaining = self.min_interval_seconds - (time.monotonic() - self._last_request)
        if remaining > 0:
            time.sleep(remaining)
        self._last_request = time.monotonic()

    def _request_bytes(self, path: str, params: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
        outbound = dict(params)
        outbound["Subscription-Key"] = self.api_key
        url = f"{self.api_base}/{path.lstrip('/')}?{urlencode(outbound)}"
        headers = {
            "Accept": "application/json, application/zip, */*;q=0.5",
            "Accept-Encoding": "identity",
            "User-Agent": self.user_agent,
        }
        for attempt in range(self.retries + 1):
            try:
                self._wait()
                with urlopen(Request(url, headers=headers), timeout=self.timeout_seconds) as response:
                    declared = response.headers.get("Content-Length")
                    if declared and int(declared) > self.max_bytes:
                        raise EdinetError("EDINET response exceeds configured size limit")
                    data = response.read(self.max_bytes + 1)
                    if len(data) > self.max_bytes:
                        raise EdinetError("EDINET response exceeds configured size limit")
                    return int(response.status), dict(response.headers.items()), data
            except HTTPError as error:
                if error.code not in {408, 425, 429, 500, 502, 503, 504} or attempt >= self.retries:
                    raise
            except URLError:
                if attempt >= self.retries:
                    raise
            time.sleep(min(2**attempt, 8))
        raise RuntimeError("EDINET retry loop exited unexpectedly")

    def _store_object(self, data: bytes, filename: str) -> tuple[str, str, bool]:
        checksum = hashlib.sha256(data).hexdigest()
        cached = self.state["objects_by_sha256"].get(checksum)
        if cached:
            path = validated_object_path(self.lake_root, str(cached))
            if path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == checksum:
                return checksum, str(cached), False
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._")[:120]
        if not safe_name:
            safe_name = "edinet.bin"
        relative = Path("raw") / "objects" / f"sha256={checksum[:2]}" / f"{checksum}__{safe_name}"
        destination = validated_object_path(self.lake_root, relative.as_posix())
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
            try:
                with temporary.open("wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary.replace(destination)
            finally:
                if temporary.exists():
                    temporary.unlink()
        if hashlib.sha256(destination.read_bytes()).hexdigest() != checksum:
            raise ValueError(f"Stored EDINET object checksum mismatch: {destination}")
        self.state["objects_by_sha256"][checksum] = relative.as_posix()
        return checksum, relative.as_posix(), True

    def fetch(
        self,
        *,
        path: str,
        params: dict[str, str],
        filename: str,
        source_id: str,
        company: str,
        company_id: str | None,
        category: str,
        parent_url: str | None = None,
        allow_cache: bool = False,
    ) -> tuple[FetchRecord, bytes | None]:
        safe_url = f"{self.api_base}/{path.lstrip('/')}?{urlencode(params)}"
        retrieved = utc_now()
        record = FetchRecord(
            source_id=source_id,
            company=company,
            company_id=company_id,
            category=category,
            url=safe_url,
            request_method="GET",
            parent_url=parent_url,
            retrieved_at=isoformat(retrieved),
            status="error",
        )
        cache_key = f"EDINET GET {safe_url}"
        try:
            cached = self.state["urls"].get(cache_key, {})
            if allow_cache and cached.get("object_path") and cached.get("checksum_sha256"):
                cached_path = validated_object_path(self.lake_root, str(cached["object_path"]))
                if cached_path.is_file() and hashlib.sha256(cached_path.read_bytes()).hexdigest() == cached["checksum_sha256"]:
                    record.status = "unchanged"
                    record.http_status = 200
                    record.final_url = safe_url
                    record.content_type = cached.get("content_type")
                    record.size_bytes = cached_path.stat().st_size
                    record.checksum_sha256 = cached["checksum_sha256"]
                    record.object_path = cached["object_path"]
                    return record, cached_path.read_bytes()

            status, headers, data = self._request_bytes(path, params)
            content_type = str(headers.get("Content-Type") or headers.get("content-type") or "application/octet-stream").split(";", 1)[0]
            checksum, object_path, created = self._store_object(data, filename)
            previous = self.state["urls"].get(cache_key, {})
            record.status = "fetched" if created else "unchanged"
            if previous.get("checksum_sha256") == checksum:
                record.status = "unchanged"
            record.http_status = status
            record.final_url = safe_url
            record.content_type = content_type
            record.size_bytes = len(data)
            record.checksum_sha256 = checksum
            record.object_path = object_path
            self.state["urls"][cache_key] = {
                "checksum_sha256": checksum,
                "object_path": object_path,
                "content_type": content_type,
                "size_bytes": len(data),
                "last_seen_at": isoformat(retrieved),
            }
            return record, data
        except (HTTPError, URLError, OSError, ValueError, EdinetError) as error:
            message = f"{type(error).__name__}: {error}".replace(self.api_key, "[REDACTED]")
            record.error = message
            if isinstance(error, HTTPError):
                record.http_status = error.code
            return record, None

    @staticmethod
    def parse_list_response(data: bytes) -> list[dict[str, Any]]:
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise EdinetError("EDINET document list returned invalid JSON") from error
        metadata = payload.get("metadata") if isinstance(payload, dict) else None
        status = str((metadata or {}).get("status") or payload.get("status") or "") if isinstance(payload, dict) else ""
        if status not in {"200", ""}:
            message = (metadata or {}).get("message") or payload.get("message") or "unknown error"
            raise EdinetError(f"EDINET API error {status}: {message}")
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            raise EdinetError("EDINET document list has no results array")
        return [row for row in results if isinstance(row, dict)]

    @staticmethod
    def validate_zip_response(data: bytes, doc_id: str, kind: str) -> None:
        if data.startswith(b"PK\x03\x04"):
            return
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = None
        if isinstance(payload, dict):
            metadata = payload.get("metadata") or payload
            raise EdinetError(
                f"EDINET {kind} download failed for {doc_id}: "
                f"{metadata.get('status', '')} {metadata.get('message', '')}".strip()
            )
        raise EdinetError(f"EDINET {kind} response for {doc_id} is not a ZIP file")

    def run(
        self,
        companies: list[EdinetCompany],
        start: date,
        end: date,
        *,
        history_years: int = 5,
        refresh: bool = False,
    ) -> dict[str, Any]:
        lock_path = self.lake_root / "metadata" / "collector.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                self.state = self._load_state()
                return self._run_locked(companies, start, end, history_years, refresh)
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _run_locked(
        self,
        companies: list[EdinetCompany],
        start: date,
        end: date,
        history_years: int,
        refresh: bool,
    ) -> dict[str, Any]:
        started = utc_now()
        run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        results: list[FetchRecord] = []
        list_items: list[dict[str, Any]] = []
        list_failed = False
        recent_boundary = end - timedelta(days=6)
        for target_date in date_range(start, end):
            params = {"date": target_date.isoformat(), "type": "2"}
            record, data = self.fetch(
                path="documents.json",
                params=params,
                filename=f"edinet_documents_{target_date.isoformat()}.json",
                source_id="edinet_documents_list",
                company="",
                company_id=None,
                category="regulatory_index",
                allow_cache=not refresh and target_date < recent_boundary,
            )
            results.append(record)
            if data is None:
                list_failed = True
                continue
            try:
                list_items.extend(self.parse_list_response(data))
            except EdinetError as error:
                record.status = "error"
                record.error = str(error)
                list_failed = True

        filings = select_filings(list_items, companies, history_years)
        company_map = {company.company_id: company for company in companies}
        manifest_filings: list[dict[str, Any]] = []
        document_failed = False
        for filing in filings:
            company = company_map[filing["company_id"]]
            doc_id = str(filing["docID"])
            parent = f"{self.api_base}/documents.json"
            stored: dict[str, Any] = {}
            for kind, type_value, flag in (
                ("xbrl", "1", filing.get("xbrlFlag")),
                ("csv", "5", filing.get("csvFlag")),
            ):
                if str(flag or "0") != "1":
                    stored[f"{kind}_status"] = "unavailable"
                    continue
                record, data = self.fetch(
                    path=f"documents/{doc_id}",
                    params={"type": type_value},
                    filename=f"{doc_id}_type{type_value}_{kind}.zip",
                    source_id=f"{company.company_id}_edinet_{kind}",
                    company=company.display_name,
                    company_id=company.company_id,
                    category="regulatory_filing",
                    parent_url=parent,
                    allow_cache=not refresh,
                )
                results.append(record)
                if data is not None:
                    try:
                        self.validate_zip_response(data, doc_id, kind)
                    except EdinetError as error:
                        record.status = "error"
                        record.error = str(error)
                        data = None
                if data is None:
                    document_failed = True
                    stored[f"{kind}_status"] = "error"
                else:
                    stored[f"{kind}_status"] = record.status
                    stored[f"{kind}_document_id"] = record.checksum_sha256
                    stored[f"{kind}_raw_path"] = record.object_path

            row = {
                "company_id": company.company_id,
                "company_name": company.display_name,
                "ticker": company.ticker,
                "edinet_code": company.edinet_code,
                "doc_id": doc_id,
                "doc_type_code": str(filing.get("docTypeCode") or ""),
                "report_type": filing["report_type"],
                "fiscal_year": filing.get("fiscal_year"),
                "period_start": filing.get("periodStart"),
                "period_end": filing.get("periodEnd"),
                "submitted_at": filing.get("submitDateTime"),
                "filer_name": filing.get("filerName"),
                "description": filing.get("docDescription"),
                "parent_doc_id": filing.get("parentDocID"),
                "is_amendment": str(filing.get("docTypeCode")) in CORRECTION_DOC_TYPES,
                "legal_status": filing.get("legalStatus"),
                "withdrawal_status": filing.get("withdrawalStatus"),
                "accounting_standard_expected": company.accounting_standard,
                **stored,
            }
            row["collection_status"] = (
                "succeeded"
                if row.get("xbrl_document_id") and row.get("csv_document_id")
                else "xbrl_only"
                if row.get("xbrl_document_id")
                else "failed"
            )
            manifest_filings.append(row)

        finished = utc_now()
        counts: dict[str, int] = {}
        for result in results:
            counts[result.status] = counts.get(result.status, 0) + 1
        manifest = {
            "manifest_version": 1,
            "pipeline_name": "edinet_financial_collection",
            "run_id": run_id,
            "started_at": isoformat(started),
            "finished_at": isoformat(finished),
            "source_count": len(companies),
            "request_count": len(results),
            "status_counts": counts,
            "date_range": {"start": start.isoformat(), "end": end.isoformat()},
            "history_years": history_years,
            "company_ids": [company.company_id for company in companies],
            "results": [asdict(result) for result in results],
            "filings": manifest_filings,
        }
        if not list_failed and not document_failed:
            self.state["edinet"]["last_successful_list_date"] = end.isoformat()
        atomic_json_write(self.state_path, self.state)
        manifest_path = self.lake_root / "metadata" / "manifests_json"
        manifest_path /= started.strftime("%Y/%m/%d")
        manifest_path /= f"{run_id}.json"
        atomic_json_write(manifest_path, manifest)
        manifest["manifest_path"] = str(manifest_path)
        return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect annual and semiannual Japanese game-company filings from EDINET."
    )
    parser.add_argument("--companies", type=Path, default=DEFAULT_COMPANIES)
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--company", action="append", default=[])
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--history-years", type=int, default=5)
    parser.add_argument(
        "--code-list",
        type=Path,
        help="Optional official Edinetcode.zip or EdinetcodeDlInfo.csv used to validate configured identifiers.",
    )
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--list-companies", action="store_true")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--api-key-env", default=DEFAULT_API_KEY_ENV)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--min-interval", type=float, default=0.25)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if not 1 <= args.history_years <= 10:
            raise ValueError("history-years must be between 1 and 10")
        companies = load_edinet_companies(args.companies)
        if args.code_list:
            validate_edinet_code_list(read_edinet_code_list(args.code_list), companies)
        if args.company:
            requested = set(args.company)
            known = {company.company_id for company in companies}
            unknown = sorted(requested - known)
            if unknown:
                raise ValueError("Unknown EDINET company ids: " + ", ".join(unknown))
            companies = [company for company in companies if company.company_id in requested]
        if args.list_companies:
            for company in companies:
                print(
                    f"{company.company_id:18} {company.ticker} "
                    f"{company.edinet_code} {company.display_name}"
                )
            return 0
        api_key = os.environ.get(args.api_key_env, "")
        if not api_key:
            raise ValueError(f"Missing required environment variable: {args.api_key_env}")
        collector = EdinetCollector(
            args.lake_root,
            api_key,
            api_base=args.api_base,
            timeout_seconds=args.timeout,
            retries=args.retries,
            min_interval_seconds=args.min_interval,
        )
        if bool(args.start_date) != bool(args.end_date):
            raise ValueError("start-date and end-date must be supplied together")
        if args.start_date:
            start, end = args.start_date, args.end_date
        else:
            start, end = collector.default_dates(date.today(), args.history_years)
        manifest = collector.run(
            companies,
            start,
            end,
            history_years=args.history_years,
            refresh=args.refresh,
        )
        print(f"Run: {manifest['run_id']}")
        print(f"Dates: {start} through {end}")
        print(f"Companies: {len(companies)}")
        print(f"Filings: {len(manifest['filings'])}")
        print(f"Statuses: {json.dumps(manifest['status_counts'], sort_keys=True)}")
        print(f"Manifest: {manifest['manifest_path']}")
        return 1 if manifest["status_counts"].get("error") else 0
    except (OSError, ValueError, json.JSONDecodeError, EdinetError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
