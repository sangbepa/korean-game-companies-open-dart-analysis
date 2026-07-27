#!/usr/bin/env python3
"""Collect Chinese game-company annual indicators and attach release notes.

The first-stage financial collector uses BaoStock for mainland A-share
companies.  BaoStock is free and needs neither an API key nor an account, but
it exposes standardized indicators rather than complete audited statements.
Official filing URLs for every company are therefore retained in the output.
"""

from __future__ import annotations

import argparse
import csv
import importlib
import json
import math
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_COMPANIES_CONFIG = ROOT / "config" / "china_game_companies.json"
DEFAULT_RELEASES_CONFIG = ROOT / "config" / "china_game_releases.json"
DEFAULT_OUTPUT_DIR = ROOT / "china_games" / "dataset"
DEFAULT_RAW_ROOT = ROOT / "china_game_lake"

BAOSTOCK_ENDPOINTS = (
    ("profit", "query_profit_data"),
    ("operation", "query_operation_data"),
    ("growth", "query_growth_data"),
    ("balance", "query_balance_data"),
    ("cash_flow", "query_cash_flow_data"),
)
IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_]{1,79}$")
BAOSTOCK_CODE_PATTERN = re.compile(r"^(?:sh|sz)\.\d{6}$")
RELEASE_DATE_PATTERN = re.compile(
    r"^\d{4}(?:-Q[1-4]|-\d{2}(?:-\d{2})?)?$"
)
MISSING_VALUES = {"", "--", "null", "none", "nan", "n/a"}
CORE_FINANCIAL_FIELDS = (
    "company_id",
    "display_name",
    "name_zh",
    "name_ko",
    "exchange",
    "ticker",
    "baostock_code",
    "fiscal_year",
    "quarter",
    "stat_date",
    "publication_date",
    "reporting_currency",
    "financial_data_status",
    "collection_error",
    "profit_MBRevenue",
    "profit_netProfit",
    "profit_roeAvg",
    "profit_npMargin",
    "profit_gpMargin",
    "growth_YOYNI",
    "growth_YOYEquity",
    "growth_YOYAsset",
    "balance_currentRatio",
    "balance_quickRatio",
    "balance_cashRatio",
    "balance_liabilityToAsset",
    "cash_flow_CFOToOR",
    "cash_flow_CFOToNP",
    "revenue_yoy_calculated",
    "net_profit_yoy_calculated",
    "official_filing_url",
    "retrieved_at_utc",
)


class ConfigurationError(ValueError):
    """Raised when a pipeline configuration is internally inconsistent."""


class CollectionError(RuntimeError):
    """Raised when BaoStock cannot return a requested result."""


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


def validate_configs(
    companies_payload: Mapping[str, Any], releases_payload: Mapping[str, Any]
) -> None:
    """Validate identifiers, period coverage, and company-year annotations."""
    period = companies_payload.get("period")
    if not isinstance(period, dict):
        raise ConfigurationError("companies config is missing period")
    start_year = int(period.get("start_year", 0))
    end_year = int(period.get("end_year", 0))
    quarter = int(period.get("quarter", 0))
    if start_year < 2000 or end_year < start_year or quarter not in {1, 2, 3, 4}:
        raise ConfigurationError("invalid collection period")

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
        if not str(company.get("official_filing_url", "")).startswith("https://"):
            raise ConfigurationError(f"missing official filing URL: {company_id}")
        if company.get("financial_collection_method") == "baostock":
            code = str(company.get("baostock_code", ""))
            if not BAOSTOCK_CODE_PATTERN.fullmatch(code):
                raise ConfigurationError(
                    f"invalid BaoStock code for {company_id}: {code!r}"
                )

    releases = releases_payload.get("releases")
    if not isinstance(releases, list):
        raise ConfigurationError("releases must be a list")
    covered_pairs: set[tuple[str, int]] = set()
    for release in releases:
        if not isinstance(release, dict):
            raise ConfigurationError("each release must be an object")
        company_id = str(release.get("company_id", ""))
        year = int(release.get("fiscal_year", 0))
        if company_id not in company_ids:
            raise ConfigurationError(f"unknown release company: {company_id}")
        if not start_year <= year <= end_year:
            raise ConfigurationError(
                f"release year outside configured range: {company_id} {year}"
            )
        if not str(release.get("title", "")).strip():
            raise ConfigurationError(f"release title is missing: {company_id} {year}")
        release_date = str(release.get("release_date", ""))
        if not RELEASE_DATE_PATTERN.fullmatch(release_date):
            raise ConfigurationError(f"invalid release date: {release_date!r}")
        if not str(release.get("source_url", "")).startswith("https://"):
            raise ConfigurationError(
                f"release source must use HTTPS: {company_id} {year}"
            )
        covered_pairs.add((company_id, year))

    expected_pairs = {
        (company_id, year)
        for company_id in company_ids
        for year in range(start_year, end_year + 1)
    }
    missing_pairs = sorted(expected_pairs - covered_pairs)
    if missing_pairs:
        formatted = ", ".join(f"{company}:{year}" for company, year in missing_pairs)
        raise ConfigurationError(f"release annotations are missing: {formatted}")


def parse_numeric(value: Any) -> float | None:
    """Convert BaoStock numeric strings while preserving absent values."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text.casefold() in MISSING_VALUES:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def query_baostock_endpoint(
    baostock: Any, method_name: str, code: str, year: int, quarter: int
) -> dict[str, str]:
    """Return one annual endpoint row as a mapping."""
    query = getattr(baostock, method_name)
    result = query(code=code, year=year, quarter=quarter)
    if str(result.error_code) != "0":
        raise CollectionError(
            f"{method_name} failed for {code} {year}Q{quarter}: "
            f"{result.error_code} {result.error_msg}"
        )
    rows: list[dict[str, str]] = []
    while result.error_code == "0" and result.next():
        rows.append(dict(zip(result.fields, result.get_row_data())))
    if not rows:
        raise CollectionError(f"{method_name} returned no row for {code} {year}Q{quarter}")
    if len(rows) > 1:
        rows.sort(key=lambda row: (row.get("statDate", ""), row.get("pubDate", "")))
    return rows[-1]


def collect_baostock_rows(
    baostock: Any,
    companies: Sequence[Mapping[str, Any]],
    start_year: int,
    end_year: int,
    quarter: int,
    request_interval: float = 0.05,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect one wide annual row and raw endpoint records."""
    retrieved_at = isoformat_utc(utc_now())
    financial_rows: list[dict[str, Any]] = []
    raw_records: list[dict[str, Any]] = []

    for company in companies:
        if company.get("financial_collection_method") != "baostock":
            continue
        for year in range(start_year, end_year + 1):
            row: dict[str, Any] = {
                "company_id": company["company_id"],
                "display_name": company["display_name"],
                "name_zh": company["name_zh"],
                "name_ko": company["name_ko"],
                "exchange": company["exchange"],
                "ticker": company["ticker"],
                "baostock_code": company["baostock_code"],
                "fiscal_year": year,
                "quarter": quarter,
                "stat_date": "",
                "publication_date": "",
                "reporting_currency": company["reporting_currency"],
                "financial_data_status": "collected_baostock",
                "collection_error": "",
                "official_filing_url": company["official_filing_url"],
                "retrieved_at_utc": retrieved_at,
            }
            errors: list[str] = []
            for endpoint_name, method_name in BAOSTOCK_ENDPOINTS:
                try:
                    endpoint_row = query_baostock_endpoint(
                        baostock,
                        method_name,
                        str(company["baostock_code"]),
                        year,
                        quarter,
                    )
                    raw_records.append(
                        {
                            "company_id": company["company_id"],
                            "baostock_code": company["baostock_code"],
                            "fiscal_year": year,
                            "quarter": quarter,
                            "endpoint": endpoint_name,
                            "method": method_name,
                            "retrieved_at_utc": retrieved_at,
                            "data": endpoint_row,
                        }
                    )
                    if endpoint_row.get("statDate"):
                        row["stat_date"] = endpoint_row["statDate"]
                    if endpoint_row.get("pubDate"):
                        row["publication_date"] = max(
                            str(row["publication_date"]), endpoint_row["pubDate"]
                        )
                    for field, raw_value in endpoint_row.items():
                        if field in {"code", "pubDate", "statDate"}:
                            continue
                        numeric = parse_numeric(raw_value)
                        row[f"{endpoint_name}_{field}"] = (
                            numeric if numeric is not None else raw_value
                        )
                except (AttributeError, CollectionError) as exc:
                    errors.append(str(exc))
                if request_interval:
                    time.sleep(request_interval)
            if errors:
                row["financial_data_status"] = (
                    "partial_baostock" if len(errors) < len(BAOSTOCK_ENDPOINTS) else "error"
                )
                row["collection_error"] = " | ".join(errors)
            financial_rows.append(row)

    add_calculated_growth(financial_rows)
    return financial_rows, raw_records


def add_calculated_growth(rows: list[dict[str, Any]]) -> None:
    """Add comparable year-over-year changes from BaoStock absolute values."""
    by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_company[str(row["company_id"])].append(row)
    for company_rows in by_company.values():
        company_rows.sort(key=lambda item: int(item["fiscal_year"]))
        previous: dict[str, Any] | None = None
        for row in company_rows:
            row["revenue_yoy_calculated"] = None
            row["net_profit_yoy_calculated"] = None
            if previous is not None:
                row["revenue_yoy_calculated"] = growth_rate(
                    previous.get("profit_MBRevenue"), row.get("profit_MBRevenue")
                )
                row["net_profit_yoy_calculated"] = growth_rate(
                    previous.get("profit_netProfit"), row.get("profit_netProfit")
                )
            previous = row


def growth_rate(previous: Any, current: Any) -> float | None:
    prior = parse_numeric(previous)
    latest = parse_numeric(current)
    if prior in {None, 0.0} or latest is None:
        return None
    return latest / prior - 1.0


def company_csv_rows(companies: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    fields = (
        "company_id",
        "legal_name",
        "display_name",
        "name_zh",
        "name_ko",
        "exchange",
        "ticker",
        "reporting_currency",
        "financial_collection_method",
        "baostock_code",
        "official_filing_url",
        "scope_note_ko",
    )
    return [{field: company.get(field, "") for field in fields} for company in companies]


def build_annual_panel(
    companies: Sequence[Mapping[str, Any]],
    releases: Sequence[Mapping[str, Any]],
    financial_rows: Sequence[Mapping[str, Any]],
    start_year: int,
    end_year: int,
) -> list[dict[str, Any]]:
    """Build an 8-company panel with release annotations and optional metrics."""
    releases_by_pair: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for release in releases:
        releases_by_pair[(str(release["company_id"]), int(release["fiscal_year"]))].append(
            release
        )
    financial_by_pair = {
        (str(row["company_id"]), int(row["fiscal_year"])): row
        for row in financial_rows
    }
    panel: list[dict[str, Any]] = []
    for company in companies:
        company_id = str(company["company_id"])
        for year in range(start_year, end_year + 1):
            notes = releases_by_pair[(company_id, year)]
            row: dict[str, Any] = {
                "company_id": company_id,
                "display_name": company["display_name"],
                "name_zh": company["name_zh"],
                "name_ko": company["name_ko"],
                "exchange": company["exchange"],
                "ticker": company["ticker"],
                "fiscal_year": year,
                "reporting_currency": company["reporting_currency"],
                "financial_collection_method": company["financial_collection_method"],
                "financial_data_status": "official_filing_registered_not_parsed",
                "representative_release_count": len(notes),
                "representative_release_titles": " | ".join(
                    str(note["title"]) for note in notes
                ),
                "representative_release_titles_zh": " | ".join(
                    str(note["title_zh"]) for note in notes
                ),
                "release_dates": " | ".join(str(note["release_date"]) for note in notes),
                "release_events": " | ".join(str(note["release_event"]) for note in notes),
                "release_annotations_ko": " | ".join(
                    str(note["annotation_ko"]) for note in notes
                ),
                "release_source_urls": " | ".join(
                    str(note["source_url"]) for note in notes
                ),
                "official_filing_url": company["official_filing_url"],
                "scope_note_ko": company["scope_note_ko"],
            }
            financial = financial_by_pair.get((company_id, year))
            if financial:
                row.update(financial)
            panel.append(row)
    return panel


def ordered_fieldnames(
    rows: Sequence[Mapping[str, Any]], preferred: Sequence[str] = ()
) -> list[str]:
    keys: set[str] = set()
    for row in rows:
        keys.update(row.keys())
    ordered = [field for field in preferred if field in keys]
    ordered.extend(sorted(keys - set(ordered)))
    return ordered


def atomic_text_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    preferred_fields: Sequence[str] = (),
) -> None:
    fieldnames = ordered_fieldnames(rows, preferred_fields)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def write_json(path: Path, payload: Any) -> None:
    atomic_text_write(
        path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    )


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    content = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows
    )
    atomic_text_write(path, content)


def import_baostock() -> Any:
    try:
        return importlib.import_module("baostock")
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "baostock is required. Run: python3 -m pip install -r requirements.txt"
        ) from exc


def run_pipeline(
    companies_config: Path,
    releases_config: Path,
    output_dir: Path,
    raw_root: Path,
    skip_financial: bool = False,
    request_interval: float = 0.05,
    baostock_module: Any | None = None,
) -> dict[str, Any]:
    companies_payload = load_json(companies_config)
    releases_payload = load_json(releases_config)
    validate_configs(companies_payload, releases_payload)
    companies = companies_payload["companies"]
    releases = releases_payload["releases"]
    period = companies_payload["period"]
    start_year = int(period["start_year"])
    end_year = int(period["end_year"])
    quarter = int(period["quarter"])
    started_at = utc_now()
    run_id = started_at.strftime("%Y%m%dT%H%M%SZ")

    financial_rows: list[dict[str, Any]] = []
    raw_records: list[dict[str, Any]] = []
    if not skip_financial:
        baostock = baostock_module or import_baostock()
        login_result = baostock.login()
        if str(login_result.error_code) != "0":
            raise CollectionError(
                f"BaoStock login failed: {login_result.error_code} "
                f"{login_result.error_msg}"
            )
        try:
            financial_rows, raw_records = collect_baostock_rows(
                baostock,
                companies,
                start_year,
                end_year,
                quarter,
                request_interval=request_interval,
            )
        finally:
            baostock.logout()

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
            "title_zh",
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
        output_dir / "a_share_annual_financial_metrics.csv",
        financial_rows,
        preferred_fields=CORE_FINANCIAL_FIELDS,
    )
    write_csv(
        output_dir / "annual_panel_with_release_notes.csv",
        panel,
        preferred_fields=(
            "company_id",
            "display_name",
            "name_zh",
            "name_ko",
            "exchange",
            "ticker",
            "fiscal_year",
            "reporting_currency",
            "financial_collection_method",
            "financial_data_status",
            "profit_MBRevenue",
            "profit_netProfit",
            "profit_roeAvg",
            "profit_npMargin",
            "profit_gpMargin",
            "revenue_yoy_calculated",
            "net_profit_yoy_calculated",
            "representative_release_titles",
            "representative_release_titles_zh",
            "release_dates",
            "release_events",
            "release_annotations_ko",
            "release_source_urls",
            "official_filing_url",
            "scope_note_ko",
        ),
    )

    raw_path = raw_root / "raw" / "baostock" / f"run_id={run_id}" / "responses.jsonl"
    if raw_records:
        write_jsonl(raw_path, raw_records)
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
        "release_annotation_count": len(releases),
        "financial_row_count": len(financial_rows),
        "raw_record_count": len(raw_records),
        "financial_status_counts": dict(sorted(status_counts.items())),
        "baostock_company_count": sum(
            company["financial_collection_method"] == "baostock"
            for company in companies
        ),
        "registered_official_filing_company_count": sum(
            company["financial_collection_method"] != "baostock"
            for company in companies
        ),
        "raw_snapshot_path": raw_path.relative_to(ROOT).as_posix()
        if raw_records and raw_path.is_relative_to(ROOT)
        else str(raw_path) if raw_records else None,
        "sources": {
            "financial_indicators": "BaoStock 0.9.x public API",
            "financial_document_authority": "CNINFO, HKEX/company IR, and SEC filing URLs retained per company",
            "release_annotations": "Official annual/quarterly reports, company news, investor presentations, and official store announcements",
        },
        "limitations_ko": [
            "BaoStock 값은 표준화된 재무비율·요약지표이며 감사 재무제표 전체 계정이 아니다.",
            "홍콩·미국 상장 3개사의 수치는 아직 패널에 자동 적재하지 않았다.",
            "대표작 주석은 완전한 출시작 목록이 아니며, 재무 변동 해석용으로 회사별·연도별 대표 이벤트를 선정했다.",
            "사업 범위가 다른 그룹사 간 총매출·이익을 비교할 때 scope_note_ko를 함께 확인해야 한다."
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
    parser.add_argument(
        "--skip-financial",
        action="store_true",
        help="Build company/release tables without calling BaoStock.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.05,
        help="Seconds to pause between BaoStock endpoint calls.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    provenance = run_pipeline(
        companies_config=args.companies_config,
        releases_config=args.releases_config,
        output_dir=args.output_dir,
        raw_root=args.raw_root,
        skip_financial=args.skip_financial,
        request_interval=max(0.0, args.request_interval),
    )
    print(json.dumps(provenance, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
