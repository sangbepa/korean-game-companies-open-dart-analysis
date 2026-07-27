#!/usr/bin/env python3
"""Collect reproducible financial-statement CSVs for the Kaggle analysis.

The script uses Open DART's major-account and full-statement endpoints. Raw API
responses stay under ignored ``dart_data/``; compact, analysis-ready CSV files
are published under ``kaggle/dataset/`` without the API key.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from collect_dart import (
    DEFAULT_API_BASE,
    DEFAULT_API_KEY_ENV,
    DEFAULT_COMPANIES,
    DartClient,
    DartError,
    atomic_json_write,
    load_companies,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RAW_ROOT = PROJECT_ROOT / "dart_data" / "financial_api"
DEFAULT_DATASET_DIR = PROJECT_ROOT / "kaggle" / "dataset"

PERIODS = (
    {
        "period_label": "FY2025",
        "business_year": "2025",
        "report_code": "11011",
        "report_name": "Annual report",
    },
    {
        "period_label": "Q1_2026",
        "business_year": "2026",
        "report_code": "11013",
        "report_name": "First-quarter report",
    },
)


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    if not materialized:
        raise ValueError(f"Refusing to write an empty CSV: {path}")
    preferred = [
        "company_id",
        "company_name",
        "corp_code",
        "stock_code",
        "market",
        "reporting_currency",
        "period_label",
        "business_year",
        "report_code",
        "report_name",
        "rcept_no",
        "fs_div",
        "fs_nm",
        "sj_div",
        "sj_nm",
        "account_id",
        "account_nm",
    ]
    discovered = {key for row in materialized for key in row}
    fieldnames = [name for name in preferred if name in discovered]
    fieldnames.extend(sorted(discovered - set(fieldnames)))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(materialized)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_or_fetch(
    client: DartClient,
    raw_root: Path,
    endpoint: str,
    parameters: dict[str, str],
    *,
    company_id: str,
    period_label: str,
    refresh: bool,
) -> dict[str, Any]:
    path = raw_root / endpoint.removesuffix(".json") / company_id
    path /= f"{period_label}.json"
    if path.exists() and not refresh:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    else:
        response = client.request(endpoint, parameters)
        try:
            payload = json.loads(response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DartError(f"Open DART returned invalid JSON for {endpoint}") from error
        atomic_json_write(path, payload)
    status = str(payload.get("status", ""))
    if status != "000":
        raise DartError(
            f"Open DART error {status or 'unknown'} for {company_id} "
            f"{period_label} {endpoint}: {payload.get('message', '')}"
        )
    if not isinstance(payload.get("list"), list):
        raise DartError(f"Open DART returned no list for {company_id} {period_label}")
    return payload


def company_rows(config_path: Path) -> list[dict[str, str]]:
    companies = load_companies(config_path)
    return [
        {
            "company_id": company.company_id,
            "company_name": company.display_name,
            "corp_code": company.corp_code,
            "stock_code": company.stock_code or "",
            "market": company.market or "",
            "reporting_currency": company.reporting_currency or "",
        }
        for company in companies
    ]


def disclosure_rows(catalog_path: Path) -> list[dict[str, Any]]:
    if not catalog_path.exists():
        raise ValueError(f"DART disclosure catalog does not exist: {catalog_path}")
    rows: list[dict[str, Any]] = []
    with catalog_path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            raw = json.loads(line)
            rows.append(
                {
                    key: raw.get(key, "")
                    for key in (
                        "company_id",
                        "corp_code",
                        "corp_name",
                        "stock_code",
                        "corp_cls",
                        "report_nm",
                        "rcept_no",
                        "flr_nm",
                        "rcept_dt",
                        "rm",
                        "viewer_url",
                    )
                }
            )
    return rows


def collect(
    client: DartClient,
    *,
    companies_path: Path,
    raw_root: Path,
    dataset_dir: Path,
    disclosure_catalog: Path,
    refresh: bool,
) -> dict[str, Any]:
    companies = load_companies(companies_path)
    all_accounts: list[dict[str, Any]] = []
    major_accounts: list[dict[str, Any]] = []
    requests = 0

    for company in companies:
        context = {
            "company_id": company.company_id,
            "company_name": company.display_name,
            "corp_code": company.corp_code,
            "stock_code": company.stock_code or "",
            "reporting_currency": company.reporting_currency or "",
        }
        for period in PERIODS:
            common_parameters = {
                "corp_code": company.corp_code,
                "bsns_year": period["business_year"],
                "reprt_code": period["report_code"],
            }
            full = load_or_fetch(
                client,
                raw_root,
                "fnlttSinglAcntAll.json",
                {**common_parameters, "fs_div": "CFS"},
                company_id=company.company_id,
                period_label=period["period_label"],
                refresh=refresh,
            )
            major = load_or_fetch(
                client,
                raw_root,
                "fnlttSinglAcnt.json",
                common_parameters,
                company_id=company.company_id,
                period_label=period["period_label"],
                refresh=refresh,
            )
            requests += 2
            period_context = {**context, **period}
            all_accounts.extend(
                {**row, **period_context, "source_endpoint": "fnlttSinglAcntAll"}
                for row in full["list"]
            )
            major_accounts.extend(
                {**row, **period_context, "source_endpoint": "fnlttSinglAcnt"}
                for row in major["list"]
                if row.get("fs_div") == "CFS"
            )

    companies_output = company_rows(companies_path)
    disclosures_output = disclosure_rows(disclosure_catalog)
    write_csv(dataset_dir / "companies.csv", companies_output)
    write_csv(dataset_dir / "financial_accounts.csv", all_accounts)
    write_csv(dataset_dir / "financial_highlights_long.csv", major_accounts)
    write_csv(dataset_dir / "disclosures_2026.csv", disclosures_output)

    collected_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    currencies = sorted(
        {
            company.reporting_currency
            for company in companies
            if company.reporting_currency
        }
    )
    manifest = {
        "schema_version": 1,
        "collected_at": collected_at,
        "source": "Financial Supervisory Service Open DART",
        "source_base_url": "https://opendart.fss.or.kr/api",
        "company_count": len(companies),
        "periods": list(PERIODS),
        "api_request_count": requests,
        "row_counts": {
            "companies.csv": len(companies_output),
            "financial_accounts.csv": len(all_accounts),
            "financial_highlights_long.csv": len(major_accounts),
            "disclosures_2026.csv": len(disclosures_output),
        },
        "statement_scope": "Consolidated financial statements (CFS)",
        "currency": currencies[0] if len(currencies) == 1 else None,
        "currencies": currencies,
        "api_key_persisted": False,
    }
    atomic_json_write(dataset_dir / "provenance.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Open DART financial statements for the Kaggle dataset."
    )
    parser.add_argument("--companies", type=Path, default=DEFAULT_COMPANIES)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument(
        "--disclosure-catalog",
        type=Path,
        default=PROJECT_ROOT / "dart_data" / "disclosures.jsonl",
    )
    parser.add_argument("--api-key-env", default=DEFAULT_API_KEY_ENV)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--min-interval", type=float, default=0.25)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        api_key = os.environ.get(args.api_key_env, "")
        if not api_key:
            raise ValueError(f"Set {args.api_key_env} before collecting financial data")
        client = DartClient(
            api_key,
            api_base=args.api_base,
            min_interval_seconds=args.min_interval,
        )
        manifest = collect(
            client,
            companies_path=args.companies,
            raw_root=args.raw_root,
            dataset_dir=args.dataset_dir,
            disclosure_catalog=args.disclosure_catalog,
            refresh=args.refresh,
        )
        print(json.dumps(manifest["row_counts"], ensure_ascii=False, sort_keys=True))
        print(f"Dataset: {args.dataset_dir.resolve()}")
        return 0
    except (DartError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
