#!/usr/bin/env python3
"""Collect Japanese game-company financials and attach fiscal-year releases.

The initial company is Capcom. Normalized figures come from the structured
table embedded in Capcom's official IR page, while annual securities report
PDFs are retained as primary-document snapshots. EDINET document metadata can
optionally be added when the caller provides an EDINET API key.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_COMPANIES_CONFIG = ROOT / "config" / "japan_game_companies.json"
DEFAULT_RELEASES_CONFIG = ROOT / "config" / "japan_game_releases.json"
DEFAULT_OUTPUT_DIR = ROOT / "japan_games" / "dataset"
DEFAULT_RAW_ROOT = ROOT / "japan_game_lake"
DEFAULT_EDINET_KEY_FILE = ROOT / ".secrets" / "edinet_api_key"
EDINET_DOCUMENTS_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
EDINET_DOCUMENT_URL_TEMPLATE = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"

IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_]{1,79}$")
EDINET_CODE_PATTERN = re.compile(r"^E\d{5}$")
EDINET_DOC_ID_PATTERN = re.compile(r"^[A-Z0-9]{8,12}$")
RELEASE_DATE_PATTERN = re.compile(r"^\d{4}(?:-Q[1-4]|-\d{2}(?:-\d{2})?)?$")
FISCAL_COLUMN_PATTERN = re.compile(r"^FY(\d{4})$")
ALLOWED_SOURCE_HOSTS = {
    "api.edinet-fsa.go.jp",
    "asia.tools.euroland.com",
    "www.capcom.co.jp",
}

METRIC_MAP: dict[tuple[str, tuple[str, ...]], str] = {
    ("Statement of Income", ("Net sales",)): "net_sales_m_jpy",
    ("Statement of Income", ("Gross profit",)): "gross_profit_m_jpy",
    ("Statement of Income", ("Operating profit",)): "operating_profit_m_jpy",
    (
        "Statement of Income",
        ("Profit before income taxes",),
    ): "profit_before_income_taxes_m_jpy",
    (
        "Statement of Income",
        ("Profit attributable to owners of the parent",),
    ): "profit_attributable_to_owners_m_jpy",
    ("Balance Sheet", ("Total assets",)): "total_assets_m_jpy",
    ("Balance Sheet", ("Current assets",)): "current_assets_m_jpy",
    ("Balance Sheet", ("Non-current assets",)): "non_current_assets_m_jpy",
    ("Balance Sheet", ("Total net assets",)): "total_net_assets_m_jpy",
    ("Balance Sheet", ("Current liabilities",)): "current_liabilities_m_jpy",
    (
        "Balance Sheet",
        ("Non-current liabilities",),
    ): "non_current_liabilities_m_jpy",
    (
        "Statement of Cash Flows",
        ("Cash flows from operating activities",),
    ): "operating_cash_flow_m_jpy",
    (
        "Statement of Cash Flows",
        ("Cash flows from investing activities",),
    ): "investing_cash_flow_m_jpy",
    (
        "Statement of Cash Flows",
        ("Cash flows from financing activities",),
    ): "financing_cash_flow_m_jpy",
    (
        "Business Segments",
        ("Net sales", "Digital Contents"),
    ): "digital_contents_net_sales_m_jpy",
    (
        "Business Segments",
        ("Operating income", "Digital Contents"),
    ): "digital_contents_operating_profit_m_jpy",
}

CORE_FINANCIAL_FIELDS = (
    "company_id",
    "display_name",
    "name_ja",
    "name_ko",
    "exchange",
    "ticker",
    "edinet_code",
    "fiscal_year",
    "fiscal_year_label",
    "fiscal_year_start",
    "fiscal_year_end",
    "reporting_currency",
    "reporting_unit",
    "financial_data_status",
    "collection_error",
    "net_sales_m_jpy",
    "gross_profit_m_jpy",
    "operating_profit_m_jpy",
    "profit_before_income_taxes_m_jpy",
    "profit_attributable_to_owners_m_jpy",
    "operating_margin",
    "net_margin",
    "revenue_yoy_calculated",
    "operating_profit_yoy_calculated",
    "net_profit_yoy_calculated",
    "total_assets_m_jpy",
    "current_assets_m_jpy",
    "non_current_assets_m_jpy",
    "total_net_assets_m_jpy",
    "current_liabilities_m_jpy",
    "non_current_liabilities_m_jpy",
    "current_ratio_calculated",
    "net_assets_to_assets_calculated",
    "operating_cash_flow_m_jpy",
    "investing_cash_flow_m_jpy",
    "financing_cash_flow_m_jpy",
    "free_cash_flow_m_jpy",
    "digital_contents_net_sales_m_jpy",
    "digital_contents_operating_profit_m_jpy",
    "digital_contents_operating_margin",
    "official_ir_data_url",
    "official_filing_url",
    "retrieved_at_utc",
)

Fetcher = Callable[[str], bytes]


class ConfigurationError(ValueError):
    """Raised when a pipeline configuration is invalid."""


class CollectionError(RuntimeError):
    """Raised when a remote source cannot be parsed or collected."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ConfigurationError(f"JSON root must be an object: {path}")
    return payload


def resolve_edinet_api_key(
    explicit_key: str | None = None,
    key_file: Path = DEFAULT_EDINET_KEY_FILE,
) -> str | None:
    """Resolve a key without logging or persisting it in generated outputs."""
    if explicit_key and explicit_key.strip():
        return explicit_key.strip()
    environment_key = os.environ.get("EDINET_API_KEY", "").strip()
    if environment_key:
        return environment_key
    if key_file.is_file():
        key = key_file.read_text(encoding="utf-8").strip()
        if not key:
            raise ConfigurationError(f"EDINET API key file is empty: {key_file}")
        return key
    return None


def validate_https_url(value: Any, label: str) -> str:
    url = str(value or "")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_SOURCE_HOSTS:
        raise ConfigurationError(f"unapproved HTTPS source for {label}: {url!r}")
    return url


def validate_configs(
    companies_payload: Mapping[str, Any], releases_payload: Mapping[str, Any]
) -> None:
    period = companies_payload.get("period")
    if not isinstance(period, dict):
        raise ConfigurationError("companies config is missing period")
    start_year = int(period.get("start_fiscal_year", 0))
    end_year = int(period.get("end_fiscal_year", 0))
    if start_year < 2000 or end_year < start_year:
        raise ConfigurationError("invalid fiscal-year period")

    companies = companies_payload.get("companies")
    if not isinstance(companies, list) or not companies:
        raise ConfigurationError("companies must be a non-empty list")
    company_ids: set[str] = set()
    for company in companies:
        if not isinstance(company, dict):
            raise ConfigurationError("each company must be an object")
        company_id = str(company.get("company_id", ""))
        if not IDENTIFIER_PATTERN.fullmatch(company_id):
            raise ConfigurationError(f"invalid company_id: {company_id!r}")
        if company_id in company_ids:
            raise ConfigurationError(f"duplicate company_id: {company_id}")
        company_ids.add(company_id)
        edinet_code = str(company.get("edinet_code", ""))
        if not EDINET_CODE_PATTERN.fullmatch(edinet_code):
            raise ConfigurationError(f"invalid EDINET code: {edinet_code!r}")
        validate_https_url(company.get("official_ir_data_url"), company_id)
        validate_https_url(company.get("official_filing_index_url"), company_id)
        filing_urls = company.get("official_filing_urls")
        if not isinstance(filing_urls, dict):
            raise ConfigurationError(f"missing filing map: {company_id}")
        for year in range(start_year, end_year + 1):
            validate_https_url(filing_urls.get(str(year)), f"{company_id} FY{year}")

    releases = releases_payload.get("releases")
    if not isinstance(releases, list):
        raise ConfigurationError("releases must be a list")
    covered_pairs: set[tuple[str, int]] = set()
    for release in releases:
        if not isinstance(release, dict):
            raise ConfigurationError("each release must be an object")
        company_id = str(release.get("company_id", ""))
        fiscal_year = int(release.get("fiscal_year", 0))
        if company_id not in company_ids:
            raise ConfigurationError(f"unknown release company: {company_id}")
        if not start_year <= fiscal_year <= end_year:
            raise ConfigurationError(
                f"release year outside configured range: {company_id} FY{fiscal_year}"
            )
        release_date = str(release.get("release_date", ""))
        if not RELEASE_DATE_PATTERN.fullmatch(release_date):
            raise ConfigurationError(f"invalid release date: {release_date!r}")
        if not str(release.get("title", "")).strip():
            raise ConfigurationError(f"missing release title: {company_id}")
        validate_https_url(release.get("source_url"), f"{company_id} release")
        covered_pairs.add((company_id, fiscal_year))

    expected_pairs = {
        (company_id, fiscal_year)
        for company_id in company_ids
        for fiscal_year in range(start_year, end_year + 1)
    }
    missing = sorted(expected_pairs - covered_pairs)
    if missing:
        detail = ", ".join(f"{company}:FY{year}" for company, year in missing)
        raise ConfigurationError(f"release annotations are missing: {detail}")


def redact_sensitive_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    redacted_query = urllib.parse.urlencode(
        [
            (key, "REDACTED" if key.casefold() == "subscription-key" else value)
            for key, value in query
        ]
    )
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, redacted_query, parsed.fragment)
    )


def default_fetcher(url: str) -> bytes:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_SOURCE_HOSTS:
        raise CollectionError(f"refusing unapproved URL: {url}")
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "make-duckdb-japan-game-pipeline/1.0",
            "Accept": "text/html,application/json,application/pdf,*/*;q=0.5",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            return response.read()
    except (urllib.error.URLError, TimeoutError) as exc:
        safe_url = redact_sensitive_url(url)
        raise CollectionError(f"failed to fetch {safe_url}: {exc}") from exc


def extract_level_zero_items(html: str) -> list[dict[str, Any]]:
    """Extract Euroland's embedded LevelZeroItems JSON without executing JS."""
    marker = ",LevelZeroItems :"
    marker_index = html.find(marker)
    if marker_index < 0:
        raise CollectionError("Euroland payload marker was not found")
    json_start = marker_index + len(marker)
    fragment = html[json_start:].lstrip()
    try:
        payload, _ = json.JSONDecoder().raw_decode(fragment)
    except json.JSONDecodeError as exc:
        raise CollectionError("Euroland LevelZeroItems JSON is invalid") from exc
    if not isinstance(payload, list):
        raise CollectionError("Euroland LevelZeroItems must be a list")
    return payload


def iter_leaf_series(
    series: Sequence[Mapping[str, Any]], prefix: tuple[str, ...] = ()
) -> Iterable[tuple[tuple[str, ...], Mapping[str, Any]]]:
    for item in series:
        name = str(item.get("name", "")).strip()
        path = prefix + (name,)
        children = item.get("ChildSeries")
        if isinstance(children, list) and children:
            yield from iter_leaf_series(children, path)
        else:
            yield path, item


def point_value(point: Any) -> float | None:
    if not isinstance(point, dict) or point.get("str") is None:
        return None
    try:
        value = float(point.get("nr"))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def fiscal_period(fiscal_year: int) -> tuple[str, str]:
    return f"{fiscal_year}-04-01", f"{fiscal_year + 1}-03-31"


def base_financial_row(
    company: Mapping[str, Any], fiscal_year: int, retrieved_at: str
) -> dict[str, Any]:
    start_date, end_date = fiscal_period(fiscal_year)
    filing_urls = company["official_filing_urls"]
    return {
        "company_id": company["company_id"],
        "display_name": company["display_name"],
        "name_ja": company["name_ja"],
        "name_ko": company["name_ko"],
        "exchange": company["exchange"],
        "ticker": company["ticker"],
        "edinet_code": company["edinet_code"],
        "fiscal_year": fiscal_year,
        "fiscal_year_label": f"FY{fiscal_year}",
        "fiscal_year_start": start_date,
        "fiscal_year_end": end_date,
        "reporting_currency": company["reporting_currency"],
        "reporting_unit": "JPY millions",
        "official_ir_data_url": company["official_ir_data_url"],
        "official_filing_url": filing_urls[str(fiscal_year)],
        "retrieved_at_utc": retrieved_at,
    }


def collect_euroland_rows(
    company: Mapping[str, Any],
    payload: Sequence[Mapping[str, Any]],
    start_year: int,
    end_year: int,
    retrieved_at: str,
) -> list[dict[str, Any]]:
    rows = {
        year: base_financial_row(company, year, retrieved_at)
        for year in range(start_year, end_year + 1)
    }
    matched_metrics: set[str] = set()
    for level_zero in payload:
        sections = level_zero.get("Data")
        if not isinstance(sections, list):
            continue
        for section in sections:
            if not isinstance(section, dict):
                continue
            section_name = str(section.get("name", ""))
            columns = section.get("Columns")
            series = section.get("Series")
            if not isinstance(columns, list) or not isinstance(series, list):
                continue
            fiscal_columns: list[int | None] = []
            for column in columns:
                match = FISCAL_COLUMN_PATTERN.fullmatch(str(column))
                fiscal_columns.append(int(match.group(1)) if match else None)
            for path, item in iter_leaf_series(series):
                output_field = METRIC_MAP.get((section_name, path))
                if output_field is None:
                    continue
                data = item.get("Data")
                if not isinstance(data, list):
                    continue
                for index, fiscal_year in enumerate(fiscal_columns):
                    if fiscal_year not in rows or index >= len(data):
                        continue
                    rows[fiscal_year][output_field] = point_value(data[index])
                    matched_metrics.add(output_field)

    required = {
        "net_sales_m_jpy",
        "operating_profit_m_jpy",
        "profit_attributable_to_owners_m_jpy",
        "total_assets_m_jpy",
        "operating_cash_flow_m_jpy",
        "digital_contents_net_sales_m_jpy",
    }
    if not required.issubset(matched_metrics):
        missing = ", ".join(sorted(required - matched_metrics))
        raise CollectionError(f"required Euroland metrics were not found: {missing}")

    result = list(rows.values())
    for row in result:
        missing_for_year = sorted(
            field for field in required if row.get(field) is None
        )
        if missing_for_year:
            row["financial_data_status"] = "partial_official_ir"
            row["collection_error"] = "missing: " + ", ".join(missing_for_year)
        else:
            row["financial_data_status"] = "collected_official_ir"
            row["collection_error"] = ""
        add_derived_metrics(row)
    add_calculated_growth(result)
    return result


def ratio(numerator: Any, denominator: Any) -> float | None:
    try:
        top = float(numerator)
        bottom = float(denominator)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(top) or not math.isfinite(bottom) or bottom == 0:
        return None
    return top / bottom


def add_derived_metrics(row: dict[str, Any]) -> None:
    row["operating_margin"] = ratio(
        row.get("operating_profit_m_jpy"), row.get("net_sales_m_jpy")
    )
    row["net_margin"] = ratio(
        row.get("profit_attributable_to_owners_m_jpy"), row.get("net_sales_m_jpy")
    )
    row["current_ratio_calculated"] = ratio(
        row.get("current_assets_m_jpy"), row.get("current_liabilities_m_jpy")
    )
    row["net_assets_to_assets_calculated"] = ratio(
        row.get("total_net_assets_m_jpy"), row.get("total_assets_m_jpy")
    )
    row["digital_contents_operating_margin"] = ratio(
        row.get("digital_contents_operating_profit_m_jpy"),
        row.get("digital_contents_net_sales_m_jpy"),
    )
    operating_cf = row.get("operating_cash_flow_m_jpy")
    investing_cf = row.get("investing_cash_flow_m_jpy")
    row["free_cash_flow_m_jpy"] = (
        float(operating_cf) + float(investing_cf)
        if operating_cf is not None and investing_cf is not None
        else None
    )


def add_calculated_growth(rows: list[dict[str, Any]]) -> None:
    rows.sort(key=lambda row: (str(row["company_id"]), int(row["fiscal_year"])))
    previous_by_company: dict[str, dict[str, Any]] = {}
    for row in rows:
        company_id = str(row["company_id"])
        previous = previous_by_company.get(company_id)
        row["revenue_yoy_calculated"] = None
        row["operating_profit_yoy_calculated"] = None
        row["net_profit_yoy_calculated"] = None
        if previous is not None:
            row["revenue_yoy_calculated"] = growth_rate(
                previous.get("net_sales_m_jpy"), row.get("net_sales_m_jpy")
            )
            row["operating_profit_yoy_calculated"] = growth_rate(
                previous.get("operating_profit_m_jpy"),
                row.get("operating_profit_m_jpy"),
            )
            row["net_profit_yoy_calculated"] = growth_rate(
                previous.get("profit_attributable_to_owners_m_jpy"),
                row.get("profit_attributable_to_owners_m_jpy"),
            )
        previous_by_company[company_id] = row


def growth_rate(previous: Any, current: Any) -> float | None:
    try:
        prior = float(previous)
        latest = float(current)
    except (TypeError, ValueError):
        return None
    if prior == 0 or not math.isfinite(prior) or not math.isfinite(latest):
        return None
    return latest / prior - 1.0


def daterange(start: str, end: str) -> Iterable[str]:
    current = date.fromisoformat(start)
    final = date.fromisoformat(end)
    if final < current or (final - current).days > 31:
        raise ConfigurationError(f"invalid EDINET search window: {start}..{end}")
    while current <= final:
        yield current.isoformat()
        current += timedelta(days=1)


def collect_edinet_metadata(
    companies: Sequence[Mapping[str, Any]],
    api_key: str,
    fetcher: Fetcher,
    request_interval: float = 0.0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect annual-report metadata; the API key is never retained."""
    records: list[dict[str, Any]] = []
    raw_responses: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    company_by_edinet = {str(c["edinet_code"]): c for c in companies}
    dates: list[str] = []
    for company in companies:
        for window in company.get("edinet_search_windows", {}).values():
            if not isinstance(window, list) or len(window) != 2:
                raise ConfigurationError("EDINET search windows require start/end dates")
            dates.extend(daterange(str(window[0]), str(window[1])))
    for filing_date in sorted(set(dates)):
        if filing_date in seen_dates:
            continue
        seen_dates.add(filing_date)
        query = urllib.parse.urlencode(
            {"date": filing_date, "type": 2, "Subscription-Key": api_key}
        )
        url = f"{EDINET_DOCUMENTS_URL}?{query}"
        payload = json.loads(fetcher(url).decode("utf-8"))
        raw_responses.append(
            {
                "date": filing_date,
                "metadata": payload.get("metadata", {}),
                "results": payload.get("results", []),
            }
        )
        for item in payload.get("results", []):
            if not isinstance(item, dict):
                continue
            company = company_by_edinet.get(str(item.get("edinetCode", "")))
            if company is None or str(item.get("docTypeCode", "")) != "120":
                continue
            records.append(
                {
                    "company_id": company["company_id"],
                    "edinet_code": company["edinet_code"],
                    "doc_id": item.get("docID"),
                    "doc_type_code": item.get("docTypeCode"),
                    "doc_description": item.get("docDescription"),
                    "submit_date_time": item.get("submitDateTime"),
                    "period_start": item.get("periodStart"),
                    "period_end": item.get("periodEnd"),
                    "xbrl_flag": item.get("xbrlFlag"),
                    "pdf_flag": item.get("pdfFlag"),
                }
            )
        if request_interval:
            time.sleep(request_interval)
    unique = {str(row["doc_id"]): row for row in records if row.get("doc_id")}
    return sorted(unique.values(), key=lambda row: str(row["submit_date_time"])), raw_responses


def download_edinet_xbrl_archives(
    documents: Sequence[Mapping[str, Any]],
    api_key: str,
    raw_run_dir: Path,
    fetcher: Fetcher,
    request_interval: float = 0.0,
) -> list[dict[str, Any]]:
    """Download type=1 EDINET filing archives and retain them without the key."""
    manifest: list[dict[str, Any]] = []
    for document in documents:
        if str(document.get("xbrl_flag", "")) != "1":
            continue
        doc_id = str(document.get("doc_id", ""))
        if not EDINET_DOC_ID_PATTERN.fullmatch(doc_id):
            raise CollectionError(f"invalid EDINET document id: {doc_id!r}")
        query = urllib.parse.urlencode({"type": 1, "Subscription-Key": api_key})
        url = f"{EDINET_DOCUMENT_URL_TEMPLATE.format(doc_id=doc_id)}?{query}"
        safe_url = redact_sensitive_url(url)
        retrieved_at = isoformat_utc(utc_now())
        try:
            body = fetcher(url)
            if not zipfile.is_zipfile(io.BytesIO(body)):
                raise CollectionError(f"EDINET document {doc_id} is not a ZIP archive")
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                names = archive.namelist()
            record = preserve_raw(
                raw_run_dir / "edinet" / f"{doc_id}.zip",
                body,
                safe_url,
                retrieved_at,
            )
            record.update(
                {
                    "doc_id": doc_id,
                    "archive_entry_count": len(names),
                    "xbrl_entry_count": sum(
                        name.casefold().endswith((".xbrl", ".xhtml", ".htm", ".html"))
                        for name in names
                    ),
                }
            )
            manifest.append(record)
        except CollectionError as exc:
            manifest.append(
                {
                    "source_url": safe_url,
                    "path": None,
                    "bytes": 0,
                    "sha256": None,
                    "retrieved_at_utc": retrieved_at,
                    "status": "error",
                    "doc_id": doc_id,
                    "error": str(exc),
                }
            )
        if request_interval:
            time.sleep(request_interval)
    return manifest


def company_csv_rows(companies: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for company in companies:
        rows.append(
            {
                key: value
                for key, value in company.items()
                if key not in {"official_filing_urls", "edinet_search_windows"}
            }
        )
    return rows


def aggregate_releases(releases: Sequence[Mapping[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    grouped: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for release in releases:
        grouped[(str(release["company_id"]), int(release["fiscal_year"]))].append(release)
    result: dict[tuple[str, int], dict[str, Any]] = {}
    for key, items in grouped.items():
        result[key] = {
            "representative_release_count": len(items),
            "representative_release_titles": " | ".join(str(i["title"]) for i in items),
            "representative_release_titles_ja": " | ".join(str(i["title_ja"]) for i in items),
            "release_dates": " | ".join(str(i["release_date"]) for i in items),
            "release_events": " | ".join(str(i["release_event"]) for i in items),
            "release_annotations_ko": " | ".join(str(i["annotation_ko"]) for i in items),
            "release_source_urls": " | ".join(str(i["source_url"]) for i in items),
        }
    return result


def build_annual_panel(
    companies: Sequence[Mapping[str, Any]],
    releases: Sequence[Mapping[str, Any]],
    financial_rows: Sequence[Mapping[str, Any]],
    start_year: int,
    end_year: int,
) -> list[dict[str, Any]]:
    financial_by_key = {
        (str(row["company_id"]), int(row["fiscal_year"])): row
        for row in financial_rows
    }
    release_by_key = aggregate_releases(releases)
    panel: list[dict[str, Any]] = []
    for company in companies:
        company_id = str(company["company_id"])
        for fiscal_year in range(start_year, end_year + 1):
            key = (company_id, fiscal_year)
            row = base_financial_row(company, fiscal_year, "")
            row.update(
                {
                    "financial_collection_method": company["financial_collection_method"],
                    "scope_note_ko": company["scope_note_ko"],
                    "financial_data_status": "not_collected",
                    "collection_error": "",
                }
            )
            row.update(financial_by_key.get(key, {}))
            row.update(release_by_key[key])
            panel.append(row)
    return panel


def ordered_fieldnames(
    rows: Sequence[Mapping[str, Any]], preferred_fields: Sequence[str] = ()
) -> list[str]:
    if not rows:
        return list(preferred_fields)
    present = {key for row in rows for key in row}
    ordered = [field for field in preferred_fields if field in present]
    ordered.extend(sorted(present - set(ordered)))
    return ordered


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    preferred_fields: Sequence[str] = (),
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ordered_fieldnames(rows, preferred_fields)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def preserve_raw(path: Path, body: bytes, source_url: str, retrieved_at: str) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return {
        "source_url": source_url,
        "path": path.relative_to(ROOT).as_posix() if path.is_relative_to(ROOT) else str(path),
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "retrieved_at_utc": retrieved_at,
        "status": "collected",
    }


def run_pipeline(
    companies_config: Path = DEFAULT_COMPANIES_CONFIG,
    releases_config: Path = DEFAULT_RELEASES_CONFIG,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    raw_root: Path = DEFAULT_RAW_ROOT,
    skip_financial: bool = False,
    skip_filings: bool = False,
    edinet_enrich: bool = False,
    edinet_api_key: str | None = None,
    edinet_key_file: Path = DEFAULT_EDINET_KEY_FILE,
    fetcher: Fetcher = default_fetcher,
    request_interval: float = 0.05,
) -> dict[str, Any]:
    companies_payload = load_json(companies_config)
    releases_payload = load_json(releases_config)
    validate_configs(companies_payload, releases_payload)
    companies = companies_payload["companies"]
    releases = releases_payload["releases"]
    period = companies_payload["period"]
    start_year = int(period["start_fiscal_year"])
    end_year = int(period["end_fiscal_year"])
    started_at = utc_now()
    retrieved_at = isoformat_utc(started_at)
    run_id = started_at.strftime("%Y%m%dT%H%M%SZ")
    raw_run_dir = raw_root / "raw" / f"run_id={run_id}"

    financial_rows: list[dict[str, Any]] = []
    raw_manifest: list[dict[str, Any]] = []
    if not skip_financial:
        for company in companies:
            source_url = str(company["official_ir_data_url"])
            body = fetcher(source_url)
            raw_manifest.append(
                preserve_raw(
                    raw_run_dir / str(company["company_id"]) / "euroland_financials.html",
                    body,
                    source_url,
                    retrieved_at,
                )
            )
            payload = extract_level_zero_items(body.decode("utf-8"))
            financial_rows.extend(
                collect_euroland_rows(
                    company, payload, start_year, end_year, retrieved_at
                )
            )
            if not skip_filings:
                for fiscal_year in range(start_year, end_year + 1):
                    filing_url = str(company["official_filing_urls"][str(fiscal_year)])
                    try:
                        filing_body = fetcher(filing_url)
                        raw_manifest.append(
                            preserve_raw(
                                raw_run_dir
                                / str(company["company_id"])
                                / f"FY{fiscal_year}_annual_securities_report.pdf",
                                filing_body,
                                filing_url,
                                isoformat_utc(utc_now()),
                            )
                        )
                    except CollectionError as exc:
                        raw_manifest.append(
                            {
                                "source_url": filing_url,
                                "path": None,
                                "bytes": 0,
                                "sha256": None,
                                "retrieved_at_utc": isoformat_utc(utc_now()),
                                "status": "error",
                                "error": str(exc),
                            }
                        )
                    if request_interval:
                        time.sleep(request_interval)

    edinet_rows: list[dict[str, Any]] = []
    edinet_raw: list[dict[str, Any]] = []
    edinet_status = "not_requested"
    if edinet_enrich:
        key = resolve_edinet_api_key(edinet_api_key, edinet_key_file)
        if not key:
            raise ConfigurationError(
                "--edinet-enrich requires EDINET_API_KEY or "
                f"the local key file {edinet_key_file}"
            )
        edinet_rows, edinet_raw = collect_edinet_metadata(
            companies, key, fetcher, request_interval=request_interval
        )
        edinet_status = "collected" if edinet_rows else "no_matching_documents"
        write_jsonl(raw_run_dir / "edinet" / "document_list_responses.jsonl", edinet_raw)
        raw_manifest.extend(
            download_edinet_xbrl_archives(
                edinet_rows,
                key,
                raw_run_dir,
                fetcher,
                request_interval=request_interval,
            )
        )

    panel = build_annual_panel(
        companies, releases, financial_rows, start_year, end_year
    )
    write_csv(output_dir / "companies.csv", company_csv_rows(companies))
    write_csv(
        output_dir / "representative_releases.csv",
        releases,
        preferred_fields=(
            "company_id",
            "fiscal_year",
            "title",
            "title_ja",
            "release_date",
            "release_date_precision",
            "release_event",
            "release_scope",
            "platform",
            "annotation_ko",
            "source_type",
            "source_url",
        ),
    )
    write_csv(
        output_dir / "annual_financial_metrics.csv",
        financial_rows,
        preferred_fields=CORE_FINANCIAL_FIELDS,
    )
    write_csv(
        output_dir / "annual_panel_with_release_notes.csv",
        panel,
        preferred_fields=CORE_FINANCIAL_FIELDS
        + (
            "financial_collection_method",
            "representative_release_titles",
            "representative_release_titles_ja",
            "release_dates",
            "release_events",
            "release_annotations_ko",
            "release_source_urls",
            "scope_note_ko",
        ),
    )
    write_csv(
        output_dir / "edinet_documents.csv",
        edinet_rows,
        preferred_fields=(
            "company_id",
            "edinet_code",
            "doc_id",
            "doc_type_code",
            "doc_description",
            "submit_date_time",
            "period_start",
            "period_end",
            "xbrl_flag",
            "pdf_flag",
        ),
    )
    if raw_manifest:
        write_jsonl(raw_run_dir / "manifest.jsonl", raw_manifest)

    status_counts: dict[str, int] = defaultdict(int)
    for row in financial_rows:
        status_counts[str(row["financial_data_status"])] += 1
    provenance = {
        "schema_version": 1,
        "run_id": run_id,
        "started_at_utc": isoformat_utc(started_at),
        "completed_at_utc": isoformat_utc(utc_now()),
        "period": period,
        "company_count": len(companies),
        "financial_row_count": len(financial_rows),
        "financial_status_counts": dict(sorted(status_counts.items())),
        "release_annotation_count": len(releases),
        "raw_document_count": sum(row["status"] == "collected" for row in raw_manifest),
        "raw_download_error_count": sum(row["status"] == "error" for row in raw_manifest),
        "edinet_api_status": edinet_status,
        "edinet_document_count": len(edinet_rows),
        "edinet_xbrl_archive_count": sum(
            row.get("status") == "collected"
            and str(row.get("path", "")).endswith(".zip")
            for row in raw_manifest
        ),
        "raw_snapshot_path": (
            raw_run_dir.relative_to(ROOT).as_posix()
            if raw_run_dir.is_relative_to(ROOT)
            else str(raw_run_dir)
        )
        if raw_manifest or edinet_raw
        else None,
        "sources": {
            "normalized_financials": "Capcom official IR Euroland structured table",
            "primary_filings": "Annual securities report PDFs published by Capcom IR after EDINET submission",
            "optional_metadata": "EDINET API v2 document list",
            "release_annotations": "Capcom official product and financial announcements",
        },
        "limitations_ko": [
            "FY 표기는 시작연도 기준이며 FY2024는 2024년 4월부터 2025년 3월까지다.",
            "금액 단위는 백만 엔이며 연결 기준이다.",
            "캡콤 전체 실적에는 아케이드·오락기기·기타 사업이 포함된다.",
            "정규화 수치는 공식 IR 표에서 수집하고 유가증권보고서 PDF를 원문 검증용으로 함께 보존한다.",
        ],
    }
    write_json(output_dir / "provenance.json", provenance)
    return provenance


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--companies-config", type=Path, default=DEFAULT_COMPANIES_CONFIG)
    parser.add_argument("--releases-config", type=Path, default=DEFAULT_RELEASES_CONFIG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--skip-financial", action="store_true")
    parser.add_argument("--skip-filings", action="store_true")
    parser.add_argument("--edinet-enrich", action="store_true")
    parser.add_argument("--edinet-key-file", type=Path, default=DEFAULT_EDINET_KEY_FILE)
    parser.add_argument("--request-interval", type=float, default=0.05)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    provenance = run_pipeline(
        companies_config=args.companies_config,
        releases_config=args.releases_config,
        output_dir=args.output_dir,
        raw_root=args.raw_root,
        skip_financial=args.skip_financial,
        skip_filings=args.skip_filings,
        edinet_enrich=args.edinet_enrich,
        edinet_key_file=args.edinet_key_file,
        request_interval=max(0.0, args.request_interval),
    )
    print(json.dumps(provenance, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
