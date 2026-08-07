#!/usr/bin/env python3
"""Normalize EDINET XBRL/CSV archives into Financial Silver and TTM Gold."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import sys
import uuid
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence
from xml.etree import ElementTree

import duckdb

from build_lakehouse_metadata import sha256_file, sql_literal, write_table
from collect_data_lake import atomic_json_write, validated_object_path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_LAKE_ROOT = PROJECT_ROOT / "game_accounting_lake"
DEFAULT_COMPANIES = PROJECT_ROOT / "config" / "companies.json"
DEFAULT_ACCOUNTS = PROJECT_ROOT / "config" / "edinet_accounts.json"
PIPELINE_VERSION = "1.0.0"
XBRLI = "http://www.xbrl.org/2003/instance"
XBRLDI = "http://xbrl.org/2006/xbrldi"
XLINK = "http://www.w3.org/1999/xlink"
MAX_ZIP_ENTRIES = 20_000
MAX_ZIP_UNCOMPRESSED = 2 * 1024 * 1024 * 1024
FLOW_ACCOUNTS = {
    "revenue",
    "operating_income",
    "net_income_parent",
    "operating_cash_flow",
    "capital_expenditure",
    "research_and_development_expense",
}
INSTANT_ACCOUNTS = {
    "total_assets",
    "total_liabilities",
    "total_equity",
    "cash_and_cash_equivalents",
}
SEGMENT_ACCOUNTS = {"segment_revenue", "segment_profit"}


FILINGS_SCHEMA = (
    ("filing_id", "VARCHAR"),
    ("company_id", "VARCHAR"),
    ("edinet_code", "VARCHAR"),
    ("ticker", "VARCHAR"),
    ("filer_name", "VARCHAR"),
    ("doc_type_code", "VARCHAR"),
    ("report_type", "VARCHAR"),
    ("fiscal_year", "INTEGER"),
    ("period_start", "DATE"),
    ("period_end", "DATE"),
    ("submitted_at", "TIMESTAMPTZ"),
    ("parent_doc_id", "VARCHAR"),
    ("is_amendment", "BOOLEAN"),
    ("legal_status", "VARCHAR"),
    ("accounting_standard_expected", "VARCHAR"),
    ("xbrl_document_id", "VARCHAR"),
    ("csv_document_id", "VARCHAR"),
    ("xbrl_raw_path", "VARCHAR"),
    ("csv_raw_path", "VARCHAR"),
    ("collection_status", "VARCHAR"),
    ("is_latest_effective", "BOOLEAN"),
    ("source_manifest_path", "VARCHAR"),
)

FACT_SCHEMA = (
    ("fact_id", "VARCHAR"),
    ("company_id", "VARCHAR"),
    ("filing_id", "VARCHAR"),
    ("report_type", "VARCHAR"),
    ("fiscal_year", "INTEGER"),
    ("statement_type", "VARCHAR"),
    ("account_id", "VARCHAR"),
    ("mapping_priority", "INTEGER"),
    ("source_concept", "VARCHAR"),
    ("source_label", "VARCHAR"),
    ("context_id", "VARCHAR"),
    ("consolidation_scope", "VARCHAR"),
    ("period_type", "VARCHAR"),
    ("period_start", "DATE"),
    ("period_end", "DATE"),
    ("instant_date", "DATE"),
    ("is_comparative", "BOOLEAN"),
    ("currency", "VARCHAR"),
    ("unit_id", "VARCHAR"),
    ("numeric_value", "DECIMAL(38,4)"),
    ("decimals", "VARCHAR"),
    ("is_nil", "BOOLEAN"),
    ("taxonomy_version", "VARCHAR"),
    ("source_document_id", "VARCHAR"),
    ("source_locator", "VARCHAR"),
)

SEGMENT_SCHEMA = (
    ("segment_fact_id", "VARCHAR"),
    ("company_id", "VARCHAR"),
    ("filing_id", "VARCHAR"),
    ("report_type", "VARCHAR"),
    ("fiscal_year", "INTEGER"),
    ("segment_id", "VARCHAR"),
    ("segment_definition_version", "VARCHAR"),
    ("source_member", "VARCHAR"),
    ("metric_account_id", "VARCHAR"),
    ("mapping_priority", "INTEGER"),
    ("source_concept", "VARCHAR"),
    ("source_label", "VARCHAR"),
    ("context_id", "VARCHAR"),
    ("period_start", "DATE"),
    ("period_end", "DATE"),
    ("is_comparative", "BOOLEAN"),
    ("currency", "VARCHAR"),
    ("numeric_value", "DECIMAL(38,4)"),
    ("taxonomy_version", "VARCHAR"),
    ("source_document_id", "VARCHAR"),
    ("source_locator", "VARCHAR"),
)

RUN_SCHEMA = (
    ("run_id", "VARCHAR"),
    ("pipeline_version", "VARCHAR"),
    ("started_at", "TIMESTAMPTZ"),
    ("finished_at", "TIMESTAMPTZ"),
    ("status", "VARCHAR"),
    ("filing_count", "INTEGER"),
    ("fact_count", "BIGINT"),
    ("segment_fact_count", "BIGINT"),
    ("quality_event_count", "BIGINT"),
    ("manifest_path", "VARCHAR"),
)

QUALITY_SCHEMA = (
    ("quality_event_id", "VARCHAR"),
    ("run_id", "VARCHAR"),
    ("severity", "VARCHAR"),
    ("entity_type", "VARCHAR"),
    ("entity_id", "VARCHAR"),
    ("check_name", "VARCHAR"),
    ("message", "VARCHAR"),
)

TTM_SCHEMA = (
    ("company_id", "VARCHAR"),
    ("as_of_date", "DATE"),
    ("account_id", "VARCHAR"),
    ("basis", "VARCHAR"),
    ("currency", "VARCHAR"),
    ("numeric_value", "DECIMAL(38,4)"),
    ("annual_filing_id", "VARCHAR"),
    ("current_half_filing_id", "VARCHAR"),
    ("prior_half_filing_id", "VARCHAR"),
    ("completeness_status", "VARCHAR"),
)

SEGMENT_TTM_SCHEMA = (
    ("company_id", "VARCHAR"),
    ("segment_id", "VARCHAR"),
    ("segment_definition_version", "VARCHAR"),
    ("as_of_date", "DATE"),
    ("metric_account_id", "VARCHAR"),
    ("basis", "VARCHAR"),
    ("currency", "VARCHAR"),
    ("numeric_value", "DECIMAL(38,4)"),
    ("annual_filing_id", "VARCHAR"),
    ("current_half_filing_id", "VARCHAR"),
    ("prior_half_filing_id", "VARCHAR"),
    ("completeness_status", "VARCHAR"),
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    rendered = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(rendered)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def stable_id(*parts: Any) -> str:
    return hashlib.sha256("\x1f".join("" if part is None else str(part) for part in parts).encode()).hexdigest()


def local_name(value: str) -> str:
    if "}" in value:
        return value.rsplit("}", 1)[-1]
    if ":" in value:
        return value.rsplit(":", 1)[-1]
    return value


def normalized_token(value: str) -> str:
    return re.sub(r"[^0-9a-zぁ-んァ-ヶ一-龯]+", "", value.casefold())


def parse_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    rendered = str(value).strip().replace(",", "").replace("\u00a0", "")
    if rendered in {"-", "—", "―", "N/A", "n/a"}:
        return None
    negative = (rendered.startswith("(") and rendered.endswith(")")) or rendered.startswith("△") or rendered.startswith("▲")
    rendered = rendered.strip("()△▲ ").replace("−", "-")
    try:
        number = Decimal(rendered)
    except InvalidOperation:
        return None
    return -abs(number) if negative else number


@dataclass(frozen=True)
class AccountRule:
    account_id: str
    statement_type: str
    period_type: str
    priority: int
    concept_patterns: tuple[re.Pattern[str], ...]
    label_patterns: tuple[re.Pattern[str], ...]


class AccountMatcher:
    def __init__(self, path: Path) -> None:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        self.mapping_version = str(payload.get("mapping_version") or "unknown")
        self.rules: list[AccountRule] = []
        for raw in payload.get("accounts", []):
            self.rules.append(
                AccountRule(
                    account_id=str(raw["account_id"]),
                    statement_type=str(raw["statement_type"]),
                    period_type=str(raw["period_type"]),
                    priority=int(raw.get("priority", 100)),
                    concept_patterns=tuple(re.compile(value, re.IGNORECASE) for value in raw.get("concept_patterns", [])),
                    label_patterns=tuple(re.compile(value, re.IGNORECASE) for value in raw.get("label_patterns", [])),
                )
            )
        if not self.rules:
            raise ValueError("EDINET account mapping is empty")

    def match(self, concept: str, label: str) -> AccountRule | None:
        # EDINET concepts often embed the name of a subtotal in a different
        # concept (for example ``NonOperatingIncome`` or ``OtherNetOpeCF``).
        # Searching substrings therefore produces plausible but incorrect
        # account mappings.  Match the QName's local part and the complete
        # presentation label instead; regexes may still express intentional
        # taxonomy variants.
        concept_name = local_name(concept).strip()
        presentation_label = label.strip()
        matches = [
            rule
            for rule in self.rules
            if any(pattern.fullmatch(concept_name) for pattern in rule.concept_patterns)
            or any(pattern.fullmatch(presentation_label) for pattern in rule.label_patterns)
        ]
        return min(matches, key=lambda rule: (rule.priority, rule.account_id)) if matches else None


def load_company_segments(path: Path) -> dict[str, list[dict[str, Any]]]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    result: dict[str, list[dict[str, Any]]] = {}
    for company in payload.get("companies", []):
        if str(company.get("country_code")) != "JP":
            continue
        segments = []
        for segment in company.get("game_segments", []):
            rendered = dict(segment)
            rendered["aliases"] = [str(value) for value in segment.get("aliases", [])]
            segments.append(rendered)
        result[str(company["company_id"])] = segments
    return result


def validate_zip(path: Path) -> list[zipfile.ZipInfo]:
    try:
        archive = zipfile.ZipFile(path)
    except zipfile.BadZipFile as error:
        raise ValueError(f"Invalid ZIP archive: {path}") from error
    with archive:
        infos = archive.infolist()
    if len(infos) > MAX_ZIP_ENTRIES:
        raise ValueError(f"ZIP has too many entries: {path}")
    total = 0
    for info in infos:
        name = PurePosixPath(info.filename.replace("\\", "/"))
        if name.is_absolute() or ".." in name.parts:
            raise ValueError(f"Unsafe ZIP member path: {info.filename!r}")
        if info.external_attr >> 16 & 0o170000 == 0o120000:
            raise ValueError(f"ZIP symlink is not allowed: {info.filename!r}")
        total += info.file_size
        if total > MAX_ZIP_UNCOMPRESSED:
            raise ValueError(f"ZIP uncompressed size exceeds limit: {path}")
    return infos


def read_zip_entries(path: Path, suffixes: tuple[str, ...]) -> dict[str, bytes]:
    infos = validate_zip(path)
    result: dict[str, bytes] = {}
    with zipfile.ZipFile(path) as archive:
        for info in infos:
            if not info.is_dir() and info.filename.casefold().endswith(suffixes):
                result[info.filename] = archive.read(info)
    return result


@dataclass
class ContextInfo:
    context_id: str
    period_start: date | None
    period_end: date | None
    instant_date: date | None
    members: tuple[str, ...]


@dataclass
class XbrlFactInfo:
    concept: str
    context_id: str
    unit_id: str | None
    value: str | None
    decimals: str | None
    is_nil: bool
    locator: str


def parse_xbrl_archives(path: Path | None) -> tuple[dict[str, ContextInfo], dict[tuple[str, str, str], XbrlFactInfo], list[XbrlFactInfo], str | None]:
    if path is None:
        return {}, {}, [], None
    entries = read_zip_entries(path, (".xbrl", ".xml"))
    contexts: dict[str, ContextInfo] = {}
    fact_index: dict[tuple[str, str, str], XbrlFactInfo] = {}
    facts: list[XbrlFactInfo] = []
    taxonomy_version: str | None = None
    for filename, payload in entries.items():
        try:
            root = ElementTree.fromstring(payload)
        except ElementTree.ParseError:
            continue
        if local_name(root.tag).casefold() != "xbrl":
            continue
        if taxonomy_version is None:
            sample = payload[:200_000].decode("utf-8", errors="ignore")
            match = re.search(r"(?:taxonomy|jpcrp|ifrs)[^\"']*?(20\d{2}(?:-\d{2}-\d{2})?)", sample, re.IGNORECASE)
            taxonomy_version = match.group(1) if match else None
        for context in root.findall(f".//{{{XBRLI}}}context"):
            context_id = str(context.attrib.get("id") or "")
            if not context_id:
                continue
            start_node = context.find(f".//{{{XBRLI}}}startDate")
            end_node = context.find(f".//{{{XBRLI}}}endDate")
            instant_node = context.find(f".//{{{XBRLI}}}instant")
            members = tuple(
                str(node.text or "")
                for node in context.findall(f".//{{{XBRLDI}}}explicitMember")
                if node.text
            )
            contexts[context_id] = ContextInfo(
                context_id=context_id,
                period_start=parse_date(start_node.text if start_node is not None else None),
                period_end=parse_date(end_node.text if end_node is not None else None),
                instant_date=parse_date(instant_node.text if instant_node is not None else None),
                members=members,
            )
        for element in root.iter():
            context_id = element.attrib.get("contextRef")
            if not context_id:
                continue
            concept = local_name(element.tag)
            unit_id = element.attrib.get("unitRef")
            is_nil = str(element.attrib.get("{http://www.w3.org/2001/XMLSchema-instance}nil") or "false").casefold() == "true"
            info = XbrlFactInfo(
                concept=concept,
                context_id=context_id,
                unit_id=unit_id,
                value=None if is_nil else (element.text or "").strip(),
                decimals=element.attrib.get("decimals"),
                is_nil=is_nil,
                locator=f"{filename}#{concept}[context={context_id}]",
            )
            facts.append(info)
            fact_index[(concept.casefold(), context_id, str(unit_id or ""))] = info
    return contexts, fact_index, facts, taxonomy_version


CSV_HEADERS = {
    "concept": ("要素ID", "Element ID", "element_id"),
    "label": ("項目名", "Item Name", "item_name"),
    "context": ("コンテキストID", "Context ID", "context_id"),
    "relative": ("相対年度", "Relative Year", "relative_year"),
    "consolidation": ("連結・個別", "Consolidated/Non-consolidated", "consolidation"),
    "period": ("期間・時点", "Period/Instant", "period_or_instant"),
    "unit_id": ("ユニットID", "Unit ID", "unit_id"),
    "unit": ("単位", "Unit", "unit"),
    "value": ("値", "Value", "value"),
}


def row_value(row: dict[str, str], name: str) -> str:
    for candidate in CSV_HEADERS[name]:
        if candidate in row:
            return str(row.get(candidate) or "")
    return ""


def parse_csv_archive(path: Path) -> list[dict[str, Any]]:
    entries = read_zip_entries(path, (".csv",))
    rows: list[dict[str, Any]] = []
    for filename, payload in entries.items():
        decoded: str | None = None
        for encoding in ("utf-16", "utf-16le", "utf-8-sig", "cp932"):
            try:
                decoded = payload.decode(encoding)
                break
            except UnicodeError:
                continue
        if decoded is None:
            raise ValueError(f"Unsupported EDINET CSV encoding: {filename}")
        reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
        if not reader.fieldnames:
            continue
        reader.fieldnames = [str(value).lstrip("\ufeff") for value in reader.fieldnames]
        for row_number, row in enumerate(reader, start=2):
            rows.append(
                {
                    "concept": row_value(row, "concept"),
                    "label": row_value(row, "label"),
                    "context_id": row_value(row, "context"),
                    "relative_year": row_value(row, "relative"),
                    "consolidation": row_value(row, "consolidation"),
                    "period_hint": row_value(row, "period"),
                    "unit_id": row_value(row, "unit_id"),
                    "unit": row_value(row, "unit"),
                    "value": row_value(row, "value"),
                    "locator": f"{filename}:row={row_number}",
                }
            )
    return rows


def context_is_comparative(context_id: str, relative_year: str) -> bool:
    value = f"{context_id} {relative_year}".casefold()
    return "prior" in value or "previous" in value or "前期" in value or "前年" in value


def consolidation_scope(context_id: str, configured: str) -> str:
    value = f"{context_id} {configured}".casefold()
    if "nonconsolidated" in value or "個別" in value:
        return "separate"
    if "consolidated" in value or "連結" in value:
        return "consolidated"
    return "consolidated"


def business_members(context: ContextInfo | None) -> tuple[str, ...]:
    if context is None:
        return ()
    ignored = {
        "consolidatedmember",
        "nonconsolidatedmember",
        "totalmember",
        "allmember",
    }
    return tuple(member for member in context.members if local_name(member).casefold() not in ignored)


def match_game_segment(company_id: str, context: ContextInfo | None, segments: dict[str, list[dict[str, Any]]]) -> tuple[dict[str, Any] | None, str | None]:
    members = business_members(context)
    if not members:
        return None, None
    haystack = normalized_token(" ".join((*members, context.context_id if context else "")))
    for segment in segments.get(company_id, []):
        if segment.get("use_consolidated"):
            continue
        for alias in segment.get("aliases", []):
            token = normalized_token(str(alias))
            if token and token in haystack:
                return segment, ",".join(members)
    return None, ",".join(members)


def reported_period_from_contexts(
    contexts: dict[str, ContextInfo],
    report_type: str,
    api_period_start: Any,
) -> tuple[date, date] | None:
    """Return the actual statement period rather than EDINET list metadata.

    For doc type 160 EDINET may expose the fiscal-year end in ``periodEnd``
    even though the filing contains six-month statements.  The XBRL primary
    current-duration context is authoritative for the reported period.
    """
    start_hint = parse_date(api_period_start)
    expected_days = (300, 430) if report_type == "annual" else (120, 230)
    candidates: list[ContextInfo] = []
    for context in contexts.values():
        if not context.period_start or not context.period_end:
            continue
        duration_days = (context.period_end - context.period_start).days
        if not expected_days[0] <= duration_days <= expected_days[1]:
            continue
        if start_hint and context.period_start != start_hint:
            continue
        context_token = context.context_id.casefold()
        if report_type == "semiannual":
            is_current_period = "prior" not in context_token and (
                "interimduration" in context_token
                or ("current" in context_token and "duration" in context_token)
            )
        else:
            is_current_period = "current" in context_token and "duration" in context_token
        if not is_current_period:
            continue
        candidates.append(context)
    if not candidates:
        return None
    chosen = min(
        candidates,
        key=lambda context: (
            0 if context.context_id.casefold() in {"currentyearduration", "interimduration"} else 1,
            len(context.members),
            context.context_id,
        ),
    )
    return chosen.period_start, chosen.period_end  # type: ignore[return-value]


def mark_latest_effective(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row["is_latest_effective"] = False
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (row.get("company_id"), row.get("report_type"), row.get("period_start"), row.get("period_end"))
        groups[key].append(row)
    for group_rows in groups.values():
        ordered = sorted(
            group_rows,
            key=lambda row: (parse_datetime(row.get("submitted_at")) or datetime.min.replace(tzinfo=timezone.utc), str(row.get("doc_id"))),
        )
        eligible = [row for row in ordered if str(row.get("legal_status") or "1") != "0" and row.get("collection_status") != "failed"]
        if eligible:
            eligible[-1]["is_latest_effective"] = True


def load_filing_manifests(lake_root: Path) -> list[dict[str, Any]]:
    manifest_root = lake_root / "metadata" / "manifests_json"
    latest: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for path in sorted(manifest_root.rglob("*.json")):
        with path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
        if manifest.get("pipeline_name") != "edinet_financial_collection":
            continue
        stamp = parse_datetime(manifest.get("finished_at")) or datetime.min.replace(tzinfo=timezone.utc)
        for raw in manifest.get("filings", []):
            if not isinstance(raw, dict) or not raw.get("doc_id"):
                continue
            row = dict(raw)
            row["source_manifest_path"] = path.relative_to(lake_root).as_posix()
            previous = latest.get(str(row["doc_id"]))
            if previous is None or stamp >= previous[0]:
                latest[str(row["doc_id"])] = (stamp, row)
    rows = [value[1] for value in latest.values()]
    mark_latest_effective(rows)
    return sorted(rows, key=lambda row: (str(row.get("company_id")), str(row.get("period_end")), str(row.get("doc_id"))))


def filing_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("doc_id"), row.get("company_id"), row.get("edinet_code"), row.get("ticker"),
        row.get("filer_name"), row.get("doc_type_code"), row.get("report_type"), row.get("fiscal_year"),
        parse_date(row.get("period_start")), parse_date(row.get("period_end")), parse_datetime(row.get("submitted_at")),
        row.get("parent_doc_id"), bool(row.get("is_amendment")), row.get("legal_status"), row.get("accounting_standard_expected"),
        row.get("xbrl_document_id"), row.get("csv_document_id"), row.get("xbrl_raw_path"), row.get("csv_raw_path"),
        row.get("collection_status"), bool(row.get("is_latest_effective")), row.get("source_manifest_path"),
    )


class FinancialBuilder:
    def __init__(self, lake_root: Path, matcher: AccountMatcher, segments: dict[str, list[dict[str, Any]]], run_id: str) -> None:
        self.lake_root = lake_root
        self.matcher = matcher
        self.segments = segments
        self.run_id = run_id
        self.facts: dict[str, tuple[Any, ...]] = {}
        self.segment_facts: dict[str, tuple[Any, ...]] = {}
        self.quality: list[tuple[Any, ...]] = []
        self.reported_periods: dict[str, tuple[date, date]] = {}

    def add_quality(self, severity: str, entity_type: str, entity_id: str, check_name: str, message: str) -> None:
        quality_id = stable_id(entity_type, entity_id, check_name, message)
        self.quality.append((quality_id, self.run_id, severity, entity_type, entity_id, check_name, message))

    def _paths(self, filing: dict[str, Any]) -> tuple[Path | None, Path | None]:
        paths: list[Path | None] = []
        for key in ("xbrl_raw_path", "csv_raw_path"):
            value = filing.get(key)
            if not value:
                paths.append(None)
                continue
            path = validated_object_path(self.lake_root, str(value))
            if not path.is_file():
                raise ValueError(f"Raw EDINET object is missing: {value}")
            expected = filing.get(key.replace("raw_path", "document_id"))
            if expected and sha256_file(path) != expected:
                raise ValueError(f"Raw EDINET object checksum mismatch: {value}")
            paths.append(path)
        return paths[0], paths[1]

    def parse_filing(self, filing: dict[str, Any]) -> None:
        filing_id = str(filing["doc_id"])
        xbrl_path, csv_path = self._paths(filing)
        contexts, xbrl_index, xbrl_facts, taxonomy_version = parse_xbrl_archives(xbrl_path)
        reported_period = reported_period_from_contexts(
            contexts,
            str(filing.get("report_type") or ""),
            filing.get("period_start"),
        )
        if reported_period:
            self.reported_periods[filing_id] = reported_period
        if csv_path:
            rows = parse_csv_archive(csv_path)
        else:
            rows = [
                {
                    "concept": fact.concept,
                    "label": fact.concept,
                    "context_id": fact.context_id,
                    "relative_year": "",
                    "consolidation": "",
                    "period_hint": "",
                    "unit_id": fact.unit_id or "",
                    "unit": fact.unit_id or "",
                    "value": fact.value or "",
                    "locator": fact.locator,
                }
                for fact in xbrl_facts
            ]
        if not rows:
            raise ValueError("No EDINET financial rows were found")
        mapped_count = 0
        for row in rows:
            concept = str(row["concept"])
            label = str(row["label"])
            rule = self.matcher.match(concept, label)
            if not rule:
                continue
            value = parse_decimal(row["value"])
            if value is None:
                continue
            unit_text = f"{row.get('unit_id', '')} {row.get('unit', '')}".casefold()
            if "jpy" not in unit_text and "円" not in unit_text:
                continue
            context_id = str(row.get("context_id") or "")
            context = contexts.get(context_id)
            segment, source_member = match_game_segment(str(filing["company_id"]), context, self.segments)
            xbrl_info = xbrl_index.get((local_name(concept).casefold(), context_id, str(row.get("unit_id") or "")))
            period_start = context.period_start if context else parse_date(filing.get("period_start")) if rule.period_type == "duration" else None
            period_end = context.period_end if context else parse_date(filing.get("period_end")) if rule.period_type == "duration" else None
            instant = context.instant_date if context else parse_date(filing.get("period_end")) if rule.period_type == "instant" else None
            comparative = context_is_comparative(context_id, str(row.get("relative_year") or ""))
            source_document_id = filing.get("csv_document_id") or filing.get("xbrl_document_id")
            if segment:
                metric = rule.account_id
                if metric == "revenue":
                    metric = "segment_revenue"
                elif metric == "operating_income":
                    metric = "segment_profit"
                if metric not in SEGMENT_ACCOUNTS:
                    continue
                segment_fact_id = stable_id(filing_id, segment["segment_id"], metric, concept, context_id, value)
                self.segment_facts[segment_fact_id] = (
                    segment_fact_id, filing["company_id"], filing_id, filing.get("report_type"), filing.get("fiscal_year"),
                    segment["segment_id"], segment.get("definition_version", "current"), source_member, metric, rule.priority,
                    concept, label, context_id, period_start, period_end, comparative, "JPY", value, taxonomy_version,
                    source_document_id, row.get("locator"),
                )
                mapped_count += 1
                continue
            if business_members(context) or rule.account_id in SEGMENT_ACCOUNTS:
                continue
            scope = consolidation_scope(context_id, str(row.get("consolidation") or ""))
            if scope != "consolidated":
                continue
            fact_id = stable_id(filing_id, rule.account_id, concept, context_id, value)
            self.facts[fact_id] = (
                fact_id, filing["company_id"], filing_id, filing.get("report_type"), filing.get("fiscal_year"),
                rule.statement_type, rule.account_id, rule.priority, concept, label, context_id, scope, rule.period_type,
                period_start, period_end, instant, comparative, "JPY", row.get("unit_id"), value,
                xbrl_info.decimals if xbrl_info else None, xbrl_info.is_nil if xbrl_info else False,
                taxonomy_version, source_document_id, row.get("locator"),
            )
            mapped_count += 1
        if mapped_count == 0:
            self.add_quality("error", "filing", filing_id, "mapped_financial_facts", "No configured financial facts were mapped from the filing.")
        if not any(row[2] == filing_id for row in self.segment_facts.values()):
            configured = [segment for segment in self.segments.get(str(filing["company_id"]), []) if not segment.get("use_consolidated")]
            if configured:
                self.add_quality("warning", "filing", filing_id, "game_segment_available", "No tagged game-segment revenue or profit was found; no PDF estimate was created.")


def current_snapshot(root: Path) -> Path | None:
    current = root / "CURRENT"
    if current.is_symlink():
        resolved = current.resolve()
        snapshots = (root / "snapshots").resolve(strict=False)
        if resolved == snapshots or snapshots in resolved.parents:
            return resolved
    return None


def publish_current(root: Path, snapshot: Path) -> None:
    relative = snapshot.resolve().relative_to(root.resolve())
    temporary = root / f".CURRENT.{uuid.uuid4().hex}.tmp"
    os.symlink(relative.as_posix(), temporary)
    os.replace(temporary, root / "CURRENT")


def read_history(root: Path, name: str, schema: Sequence[tuple[str, str]]) -> list[tuple[Any, ...]]:
    active = current_snapshot(root)
    if not active or not (active / f"{name}.parquet").is_file():
        return []
    connection = duckdb.connect(":memory:")
    try:
        columns = ", ".join(f'"{column}"' for column, _ in schema)
        return connection.execute(f"SELECT {columns} FROM read_parquet(?)", [str(active / f"{name}.parquet")]).fetchall()
    finally:
        connection.close()


def fact_rows_by_id(rows: Iterable[tuple[Any, ...]], id_index: int = 0) -> dict[str, tuple[Any, ...]]:
    return {str(row[id_index]): row for row in rows}


def choose_fact(rows: list[dict[str, Any]], account_id: str, filing_id: str, *, segment_id: str | None = None) -> dict[str, Any] | None:
    matches = [
        row for row in rows
        if row["filing_id"] == filing_id
        and row.get("account_id", row.get("metric_account_id")) == account_id
        and not row["is_comparative"]
        and (segment_id is None or row.get("segment_id") == segment_id)
    ]
    if not matches:
        return None
    return min(
        matches,
        key=lambda row: (
            int(row.get("mapping_priority") or 999),
            0 if row.get("period_end") or row.get("instant_date") else 1,
            str(row.get("source_concept")),
            str(row.get("context_id")),
        ),
    )


def tuples_to_dicts(rows: Iterable[tuple[Any, ...]], schema: Sequence[tuple[str, str]]) -> list[dict[str, Any]]:
    names = [name for name, _ in schema]
    return [dict(zip(names, row)) for row in rows]


def build_ttm_rows(
    filings: list[dict[str, Any]],
    facts: list[dict[str, Any]],
    segment_facts: list[dict[str, Any]],
    company_ids: list[str],
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    output: list[tuple[Any, ...]] = []
    segment_output: list[tuple[Any, ...]] = []
    quality: list[tuple[Any, ...]] = []
    effective = [row for row in filings if row["is_latest_effective"]]
    for company_id in company_ids:
        company_filings = [row for row in effective if row["company_id"] == company_id]
        annuals = sorted([row for row in company_filings if row["report_type"] == "annual" and row["period_end"]], key=lambda row: row["period_end"])
        halves = sorted([row for row in company_filings if row["report_type"] == "semiannual" and row["period_end"]], key=lambda row: row["period_end"])
        latest_annual = annuals[-1] if annuals else None
        latest_half = halves[-1] if halves else None
        prior_half = None
        if latest_half:
            candidates = [row for row in halves[:-1] if 300 <= (latest_half["period_end"] - row["period_end"]).days <= 430]
            prior_half = candidates[-1] if candidates else None
        for account_id in sorted(FLOW_ACCOUNTS | INSTANT_ACCOUNTS):
            annual_fact = choose_fact(facts, account_id, latest_annual["filing_id"]) if latest_annual else None
            half_fact = choose_fact(facts, account_id, latest_half["filing_id"]) if latest_half else None
            prior_fact = choose_fact(facts, account_id, prior_half["filing_id"]) if prior_half else None
            value: Decimal | None = None
            basis = "ttm"
            status = "complete"
            as_of: date | None = None
            if account_id in INSTANT_ACCOUNTS:
                candidates = [fact for fact in (annual_fact, half_fact) if fact and fact.get("instant_date")]
                chosen = max(candidates, key=lambda row: row["instant_date"]) if candidates else None
                basis = "latest_instant"
                if chosen:
                    value = chosen["numeric_value"]
                    as_of = chosen["instant_date"]
                else:
                    status = "missing_latest_instant"
            elif latest_annual and (not latest_half or latest_half["period_end"] <= latest_annual["period_end"]):
                basis = "latest_annual"
                as_of = latest_annual["period_end"]
                if annual_fact:
                    value = annual_fact["numeric_value"]
                else:
                    status = "missing_annual_fact"
            elif latest_annual and latest_half:
                as_of = latest_half["period_end"]
                if annual_fact and half_fact and prior_fact:
                    value = annual_fact["numeric_value"] + half_fact["numeric_value"] - prior_fact["numeric_value"]
                else:
                    status = "missing_prior_half" if not prior_fact else "missing_required_fact"
            else:
                status = "missing_annual_filing"
                as_of = latest_half["period_end"] if latest_half else None
            output.append(
                (
                    company_id, as_of, account_id, basis, "JPY", value,
                    latest_annual["filing_id"] if latest_annual else None,
                    latest_half["filing_id"] if latest_half else None,
                    prior_half["filing_id"] if prior_half else None,
                    status,
                )
            )
            if status != "complete" and account_id in {"revenue", "operating_income", "total_assets"}:
                quality.append((company_id, account_id, status))

        definitions = sorted({(row["segment_id"], row["segment_definition_version"]) for row in segment_facts if row["company_id"] == company_id})
        for segment_id, definition in definitions:
            for metric in sorted(SEGMENT_ACCOUNTS):
                annual_fact = choose_fact(segment_facts, metric, latest_annual["filing_id"], segment_id=segment_id) if latest_annual else None
                half_fact = choose_fact(segment_facts, metric, latest_half["filing_id"], segment_id=segment_id) if latest_half else None
                prior_fact = choose_fact(segment_facts, metric, prior_half["filing_id"], segment_id=segment_id) if prior_half else None
                value = None
                status = "complete"
                basis = "ttm"
                as_of = latest_half["period_end"] if latest_half and latest_annual and latest_half["period_end"] > latest_annual["period_end"] else latest_annual["period_end"] if latest_annual else None
                if latest_annual and (not latest_half or latest_half["period_end"] <= latest_annual["period_end"]):
                    basis = "latest_annual"
                    value = annual_fact["numeric_value"] if annual_fact else None
                    if not annual_fact:
                        status = "unavailable"
                elif annual_fact and half_fact and prior_fact:
                    value = annual_fact["numeric_value"] + half_fact["numeric_value"] - prior_fact["numeric_value"]
                else:
                    status = "unavailable"
                segment_output.append(
                    (
                        company_id, segment_id, definition, as_of, metric, basis, "JPY", value,
                        latest_annual["filing_id"] if latest_annual else None,
                        latest_half["filing_id"] if latest_half else None,
                        prior_half["filing_id"] if prior_half else None,
                        status,
                    )
                )
    return output, segment_output, quality


def build_gold(lake_root: Path, financial_snapshot: Path, run_id: str, company_ids: list[str]) -> dict[str, Any]:
    connection = duckdb.connect(":memory:")
    try:
        filings = connection.execute("SELECT * FROM read_parquet(?)", [str(financial_snapshot / "filings.parquet")]).fetchall()
        facts = connection.execute("SELECT * FROM read_parquet(?)", [str(financial_snapshot / "financial_facts.parquet")]).fetchall()
        segment_facts = connection.execute("SELECT * FROM read_parquet(?)", [str(financial_snapshot / "segment_facts.parquet")]).fetchall()
    finally:
        connection.close()
    filing_dicts = tuples_to_dicts(filings, FILINGS_SCHEMA)
    fact_dicts = tuples_to_dicts(facts, FACT_SCHEMA)
    segment_dicts = tuples_to_dicts(segment_facts, SEGMENT_SCHEMA)
    ttm_rows, segment_ttm_rows, missing = build_ttm_rows(filing_dicts, fact_dicts, segment_dicts, company_ids)
    root = lake_root / "gold" / "edinet"
    snapshot = root / "snapshots" / run_id
    staging = root / ".staging" / run_id
    staging.mkdir(parents=True, exist_ok=False)
    connection = duckdb.connect(":memory:")
    try:
        write_table(connection, "company_ttm_metrics", TTM_SCHEMA, ttm_rows, staging / "company_ttm_metrics.parquet")
        write_table(connection, "game_segment_ttm_metrics", SEGMENT_TTM_SCHEMA, segment_ttm_rows, staging / "game_segment_ttm_metrics.parquet")
        quality_rows = [
            (stable_id(company, account, status), run_id, "warning", "company_account", f"{company}:{account}", "ttm_completeness", status)
            for company, account, status in missing
        ]
        write_table(connection, "data_quality_log", QUALITY_SCHEMA, quality_rows, staging / "data_quality_log.parquet")
    finally:
        connection.close()
    summary = {
        "schema_version": 1,
        "run_id": run_id,
        "built_at": isoformat(utc_now()),
        "company_count": len(company_ids),
        "ttm_metric_rows": len(ttm_rows),
        "segment_ttm_metric_rows": len(segment_ttm_rows),
        "quality_events": len(missing),
        "source_financial_snapshot": financial_snapshot.relative_to(lake_root).as_posix(),
        "snapshot_path": snapshot.relative_to(lake_root).as_posix(),
    }
    atomic_json_write(staging / "gold_build.json", summary)
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    staging.replace(snapshot)
    publish_current(root, snapshot)
    try:
        staging.parent.rmdir()
    except OSError:
        pass
    return summary


def build_financial_silver(lake_root: Path, companies_path: Path, accounts_path: Path) -> dict[str, Any]:
    lake_root = lake_root.resolve()
    matcher = AccountMatcher(accounts_path)
    segments = load_company_segments(companies_path)
    filings = load_filing_manifests(lake_root)
    started = utc_now()
    run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    builder = FinancialBuilder(lake_root, matcher, segments, run_id)
    for filing in filings:
        if filing.get("collection_status") == "failed":
            builder.add_quality("error", "filing", str(filing["doc_id"]), "raw_collection", "Required XBRL archive was not collected.")
            continue
        try:
            builder.parse_filing(filing)
        except (OSError, ValueError, zipfile.BadZipFile) as error:
            builder.add_quality("error", "filing", str(filing["doc_id"]), "financial_extraction", f"{type(error).__name__}: {error}")
    for filing in filings:
        reported_period = builder.reported_periods.get(str(filing.get("doc_id")))
        if reported_period:
            filing["period_start"], filing["period_end"] = reported_period
    mark_latest_effective(filings)
    errors = sum(row[2] == "error" for row in builder.quality)
    status = "partial" if errors else "succeeded"
    finished = utc_now()
    root = lake_root / "silver" / "financial"
    bucket = "snapshots" if status == "succeeded" else "diagnostic_snapshots"
    snapshot = root / bucket / run_id
    staging = root / ".staging" / run_id
    staging.mkdir(parents=True, exist_ok=False)
    manifest_relative = Path("silver") / "financial" / "manifests_json" / started.strftime("%Y/%m/%d") / f"{run_id}.json"
    run_row = (
        run_id, PIPELINE_VERSION, started, finished, status, len(filings), len(builder.facts), len(builder.segment_facts), len(builder.quality), manifest_relative.as_posix()
    )
    run_rows = read_history(root, "financial_runs", RUN_SCHEMA) + [run_row]
    quality_rows = read_history(root, "data_quality_log", QUALITY_SCHEMA) + builder.quality
    connection = duckdb.connect(":memory:")
    try:
        write_table(connection, "filings", FILINGS_SCHEMA, [filing_tuple(row) for row in filings], staging / "filings.parquet")
        write_table(connection, "financial_facts", FACT_SCHEMA, sorted(builder.facts.values()), staging / "financial_facts.parquet")
        write_table(connection, "segment_facts", SEGMENT_SCHEMA, sorted(builder.segment_facts.values()), staging / "segment_facts.parquet")
        write_table(connection, "financial_runs", RUN_SCHEMA, run_rows, staging / "financial_runs.parquet")
        write_table(connection, "data_quality_log", QUALITY_SCHEMA, quality_rows, staging / "data_quality_log.parquet")
    finally:
        connection.close()
    manifest = {
        "schema_version": 1,
        "pipeline_name": "edinet_financial_normalization",
        "pipeline_version": PIPELINE_VERSION,
        "run_id": run_id,
        "started_at": isoformat(started),
        "finished_at": isoformat(finished),
        "status": status,
        "mapping_version": matcher.mapping_version,
        "filing_count": len(filings),
        "financial_fact_count": len(builder.facts),
        "segment_fact_count": len(builder.segment_facts),
        "quality_event_count": len(builder.quality),
        "snapshot_path": snapshot.relative_to(lake_root).as_posix(),
        "published_to_current": status == "succeeded",
    }
    atomic_json_write(staging / "manifest.json", manifest)
    atomic_json_write(staging / "financial_build.json", manifest)
    manifest_path = lake_root / manifest_relative
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json_write(manifest_path, manifest)
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    staging.replace(snapshot)
    if status == "succeeded":
        publish_current(root, snapshot)
        gold = build_gold(lake_root, snapshot, run_id, sorted(segments))
        manifest["gold"] = gold
    try:
        staging.parent.rmdir()
    except OSError:
        pass
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build EDINET Financial Silver and JPY TTM Gold snapshots.")
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--companies", type=Path, default=DEFAULT_COMPANIES)
    parser.add_argument("--accounts", type=Path, default=DEFAULT_ACCOUNTS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = build_financial_silver(args.lake_root, args.companies, args.accounts)
    except (OSError, ValueError, json.JSONDecodeError, duckdb.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary["status"] == "succeeded" else 2


if __name__ == "__main__":
    raise SystemExit(main())
