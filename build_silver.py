#!/usr/bin/env python3
"""Build the read-only Silver extraction layer from immutable Raw objects.

The pipeline extracts document structure and searchable text. Numeric lines are
stored only as unverified reported-source candidates; they are never promoted
to reported financial facts without an explicit validation step.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import unicodedata
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit

import duckdb

from build_lakehouse_metadata import write_table
from collect_data_lake import atomic_json_write


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_LAKE_ROOT = PROJECT_ROOT / "game_accounting_lake"
DEFAULT_ACCOUNT_ALIASES = PROJECT_ROOT / "config" / "account_aliases.json"
DEFAULT_WORKER = PROJECT_ROOT / "extract_document_worker.py"
PIPELINE_VERSION = "1.0.0"
OCR_LANGUAGES = "kor+eng+jpn"
SUPPORTED_EXTENSIONS = {".pdf", ".xlsx", ".html", ".json"}
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
PERIOD_PATTERN = re.compile(
    r"^(?:(?:fy\s*)?(?:19|20)\d{2}|[1-4]q\s*(?:19|20)?\d{2}|"
    r"(?:19|20)\d{2}\s*[1-4]q|q[1-4]\s*(?:19|20)?\d{2}|"
    r"[1-4](?:st|nd|rd|th)?\s*quarter\s*(?:19|20)?\d{2})$",
    re.IGNORECASE,
)
NUMBER_TOKEN_PATTERN = re.compile(
    r"(?<![\w.])(?:\(?[-+]?\s*[$€£¥₩]?\s*\d[\d,]*(?:\.\d+)?%?\)?)"
)
URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def clean_text(value: Any) -> str:
    rendered = ("" if value is None else str(value)).replace("\x00", "")
    return rendered.encode("utf-8", errors="replace").decode("utf-8")


def collapse_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip()


def normalize_label(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", clean_text(value)).casefold()
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"[^\w&]+", " ", normalized, flags=re.UNICODE)
    return collapse_spaces(normalized)


def json_dumps(value: Any) -> str:
    def default(item: Any) -> str:
        if isinstance(item, datetime):
            return isoformat(item)
        if isinstance(item, Path):
            return str(item)
        raise TypeError(f"Object of type {type(item).__name__} is not JSON serializable")

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=default,
    )


def safe_raw_path(lake_root: Path, relative_string: str) -> Path:
    relative = Path(relative_string)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Unsafe Raw object path: {relative_string!r}")
    if relative.parts[:2] != ("raw", "objects"):
        raise ValueError(f"Raw object must be under raw/objects/: {relative_string!r}")
    root = lake_root.resolve()
    raw_objects_root = (root / "raw" / "objects").resolve(strict=False)
    if root not in raw_objects_root.parents:
        raise ValueError("Raw objects directory leaves lake root")
    candidate = (root / relative).resolve(strict=False)
    if raw_objects_root not in candidate.parents:
        raise ValueError(f"Raw object path leaves raw/objects/: {relative_string!r}")
    return candidate


def scalar_fields(value: Any) -> dict[str, Any]:
    if value is None:
        return {
            "value_kind": "null",
            "value_text": None,
            "numeric_value": None,
            "boolean_value": None,
            "date_value": None,
        }
    if isinstance(value, bool):
        return {
            "value_kind": "boolean",
            "value_text": "true" if value else "false",
            "numeric_value": None,
            "boolean_value": value,
            "date_value": None,
        }
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and not math.isfinite(value):
            return {
                "value_kind": "text",
                "value_text": str(value),
                "numeric_value": None,
                "boolean_value": None,
                "date_value": None,
            }
        return {
            "value_kind": "number",
            "value_text": str(value),
            "numeric_value": str(value),
            "boolean_value": None,
            "date_value": None,
        }
    return {
        "value_kind": "text",
        "value_text": clean_text(value),
        "numeric_value": None,
        "boolean_value": None,
        "date_value": None,
    }


class HTMLContentParser(HTMLParser):
    BLOCK_TAGS = {
        "address",
        "article",
        "aside",
        "blockquote",
        "br",
        "dd",
        "div",
        "dl",
        "dt",
        "footer",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "li",
        "main",
        "nav",
        "p",
        "section",
        "tr",
    }
    SKIP_TAGS = {"script", "style", "noscript", "svg"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.fragments: list[str] = []
        self.links: list[dict[str, Any]] = []
        self.current_link: dict[str, Any] | None = None
        self.table_cells: list[dict[str, Any]] = []
        self.table_index = 0
        self.current_table: int | None = None
        self.current_row = 0
        self.current_column = 0
        self.current_cell: dict[str, Any] | None = None
        self.cell_fragments: list[str] = []
        self.skip_depth = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        tag = tag.casefold()
        attrs_map = {name.casefold(): value for name, value in attrs}
        if tag in self.SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth:
            return
        if tag in self.BLOCK_TAGS:
            self.fragments.append("\n")
        if tag == "a" and attrs_map.get("href"):
            self.current_link = {
                "href": clean_text(attrs_map["href"]),
                "link_text": "",
            }
            self.links.append(self.current_link)
        if tag == "table":
            self.table_index += 1
            self.current_table = self.table_index
            self.current_row = 0
            self.current_column = 0
        elif tag == "tr" and self.current_table:
            self.current_row += 1
            self.current_column = 0
        elif tag in {"td", "th"} and self.current_table:
            self.fragments.append(" ")
            self.current_column += 1
            self.current_cell = {
                "table_index": self.current_table,
                "row_index": self.current_row or 1,
                "column_index": self.current_column,
                "cell_type": tag,
                "rowspan": int(attrs_map.get("rowspan") or 1),
                "colspan": int(attrs_map.get("colspan") or 1),
            }
            self.cell_fragments = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if tag in self.SKIP_TAGS and self.skip_depth:
            self.skip_depth -= 1
            return
        if self.skip_depth:
            return
        if tag in {"td", "th"} and self.current_cell:
            cell = dict(self.current_cell)
            cell["text"] = collapse_spaces(" ".join(self.cell_fragments))
            self.table_cells.append(cell)
            self.current_cell = None
            self.cell_fragments = []
            self.fragments.append(" ")
        elif tag == "table":
            self.current_table = None
            self.current_row = 0
            self.current_column = 0
        elif tag == "a":
            self.current_link = None
        if tag in self.BLOCK_TAGS:
            self.fragments.append("\n")

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        value = clean_text(data)
        self.fragments.append(value)
        if self.current_cell is not None:
            self.cell_fragments.append(value)
        if self.current_link is not None and collapse_spaces(value):
            self.current_link["link_text"] = collapse_spaces(
                f"{self.current_link['link_text']} {value}"
            )

    def text(self) -> str:
        lines = [collapse_spaces(line) for line in "".join(self.fragments).splitlines()]
        return "\n".join(line for line in lines if line)


def json_pointer_token(value: Any) -> str:
    return clean_text(value).replace("~", "~0").replace("/", "~1")


def flatten_json(value: Any, path: str = "") -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        if not value:
            yield path or "/", value
        for key, child in value.items():
            yield from flatten_json(child, f"{path}/{json_pointer_token(key)}")
    elif isinstance(value, list):
        if not value:
            yield path or "/", value
        for index, child in enumerate(value):
            yield from flatten_json(child, f"{path}/{index}")
    else:
        yield path or "/", value


def detect_language(text: str) -> str:
    sample = text[:200_000]
    hangul = len(re.findall(r"[가-힣]", sample))
    kana = len(re.findall(r"[ぁ-ゟ゠-ヿ]", sample))
    latin = len(re.findall(r"[A-Za-z]", sample))
    significant = hangul + kana + latin
    if significant == 0:
        return "unknown"
    shares = {"ko": hangul / significant, "ja": kana / significant, "en": latin / significant}
    ordered = sorted(shares.items(), key=lambda item: item[1], reverse=True)
    if ordered[0][1] >= 0.75:
        return ordered[0][0]
    if ordered[0][1] >= 0.25 and ordered[1][1] >= 0.15:
        return "mixed"
    return ordered[0][0]


def classify_document(file_extension: str, filename: str, text: str) -> str:
    haystack = f"{filename}\n{text[:80_000]}".casefold()
    if file_extension == ".json":
        return "source_api_snapshot"
    if file_extension == ".html":
        return "source_page_snapshot"
    if "fact sheet" in haystack or "fact_sheet" in haystack:
        return "financial_fact_sheet"
    if any(
        phrase in haystack
        for phrase in (
            "financial statements",
            "statements of financial position",
            "재무제표",
        )
    ):
        return "financial_statements_or_results"
    if any(
        phrase in haystack
        for phrase in (
            "earnings results",
            "financial results",
            "business results",
            "실적발표",
            "investor presentation",
        )
    ):
        return "earnings_presentation"
    return "other_ir_document"


def unit_hints(text: str) -> tuple[str | None, int | None]:
    normalized = unicodedata.normalize("NFKC", text).casefold()
    currency = None
    if re.search(r"\bkrw\b|₩|원", normalized):
        currency = "KRW"
    elif re.search(r"\bjpy\b|¥|円", normalized):
        currency = "JPY"
    elif re.search(r"\busd\b|\$", normalized):
        currency = "USD"
    elif re.search(r"\beur\b|€", normalized):
        currency = "EUR"

    scale = None
    if re.search(r"\bbn\b|\bbillion\b|십억", normalized):
        scale = 1_000_000_000
    elif re.search(r"\bmn\b|\bmillion\b|백만", normalized):
        scale = 1_000_000
    elif re.search(r"\bthousand\b|\b000s\b|천원", normalized):
        scale = 1_000
    elif "억원" in normalized:
        scale = 100_000_000
    elif currency:
        scale = 1
    return currency, scale


def statement_hint(sheet_name: str, context: str) -> str | None:
    value = f"{sheet_name} {context}".casefold()
    if re.search(r"\bbs\b|financial position|balance sheet|재무상태|貸借対照", value):
        return "balance_sheet"
    if re.search(r"\bis\b|income|profit|loss|손익|損益", value):
        return "income_statement"
    if re.search(r"\bcf\b|cash flow|현금흐름|キャッシュフロー", value):
        return "cash_flow_statement"
    if re.search(r"sales|revenue|매출|売上", value):
        return "sales_detail"
    if re.search(r"operating cost|영업비용|営業費用", value):
        return "operating_cost_detail"
    return None


def consolidation_hint(sheet_name: str, context: str) -> str | None:
    value = f"{sheet_name} {context}".casefold()
    if "consolidated" in value or "연결" in value or "連結" in value:
        return "consolidated"
    if re.search(r"\bparent\b|separate|별도|個別", value):
        return "separate_or_parent"
    return None


def parse_number_text(value: str) -> str | None:
    rendered = collapse_spaces(value)
    if not rendered:
        return None
    negative = rendered.startswith("(") and rendered.endswith(")")
    cleaned = rendered.strip("() ").replace(",", "")
    cleaned = re.sub(r"^[+$€£¥₩]\s*", "", cleaned)
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if not re.fullmatch(r"[-+]?\d+(?:\.\d+)?", cleaned):
        return None
    return f"-{cleaned}" if negative and not cleaned.startswith("-") else cleaned


class AccountMatcher:
    def __init__(self, path: Path) -> None:
        self.path = path
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("candidate_only") is not True:
            raise ValueError("Account mapping must explicitly declare candidate_only=true")
        self.mapping_version = str(payload.get("mapping_version") or "unknown")
        self.config_sha256 = sha256_file(path)
        self.accounts: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for account in payload.get("accounts", []):
            account_id = str(account["account_id"])
            if account_id in seen_ids:
                raise ValueError(f"Duplicate account_id: {account_id}")
            seen_ids.add(account_id)
            patterns = [re.compile(str(value), re.IGNORECASE) for value in account["patterns"]]
            self.accounts.append(
                {
                    "account_id": account_id,
                    "canonical_name": str(account["canonical_name"]),
                    "pattern_strings": [str(value) for value in account["patterns"]],
                    "patterns": patterns,
                }
            )
        if not self.accounts:
            raise ValueError("Account mapping has no accounts")

    def match(self, label: str) -> list[dict[str, Any]]:
        normalized = normalize_label(label)
        return [
            account
            for account in self.accounts
            if any(pattern.fullmatch(normalized) for pattern in account["patterns"])
        ]

    def parquet_rows(self) -> list[tuple[Any, ...]]:
        rows: list[tuple[Any, ...]] = []
        for account in self.accounts:
            for pattern in account["pattern_strings"]:
                rows.append(
                    (
                        self.mapping_version,
                        account["account_id"],
                        account["canonical_name"],
                        pattern,
                        True,
                    )
                )
        return rows


def worker_python_candidates(explicit: Path | None) -> list[Path]:
    values: list[Path] = []
    if explicit:
        values.append(explicit)
    configured = os.environ.get("SILVER_WORKER_PYTHON")
    if configured:
        values.append(Path(configured))
    values.append(Path(sys.executable))
    values.append(
        Path.home()
        / ".cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3"
    )
    deduplicated: list[Path] = []
    seen: set[str] = set()
    for value in values:
        rendered = str(value.expanduser().resolve(strict=False))
        if rendered not in seen:
            seen.add(rendered)
            deduplicated.append(Path(rendered))
    return deduplicated


def select_worker_python(explicit: Path | None) -> Path:
    errors: list[str] = []
    for candidate in worker_python_candidates(explicit):
        if not candidate.is_file():
            errors.append(f"{candidate}: not found")
            continue
        process = subprocess.run(
            [str(candidate), "-c", "import openpyxl, pypdf"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if process.returncode == 0:
            return candidate
        errors.append(f"{candidate}: {process.stderr.strip()[:200]}")
    raise ValueError(
        "No Python runtime with pypdf and openpyxl is available. "
        "Install requirements or pass --worker-python. Tried: "
        + "; ".join(errors)
    )


def worker_environment(
    worker_python: Path, worker_path: Path
) -> tuple[dict[str, Any], str]:
    process = subprocess.run(
        [str(worker_python), str(worker_path), "--describe-environment"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        check=False,
    )
    if process.returncode:
        raise ValueError(
            "Could not fingerprint extraction runtime: "
            + process.stderr.strip()[:1000]
        )
    try:
        payload = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise ValueError("Extraction worker returned an invalid environment record") from error
    if not isinstance(payload, dict) or payload.get("record_type") != "environment":
        raise ValueError("Extraction worker returned an invalid environment record")
    environment = {
        "orchestrator_python": sys.version,
        "ocr_languages": OCR_LANGUAGES,
        "worker": payload,
    }
    return environment, sha256_text(json_dumps(environment))


def load_inventory(lake_root: Path) -> list[dict[str, Any]]:
    metadata = lake_root / "metadata"
    documents_path = metadata / "documents.parquet"
    relations_path = metadata / "source_documents.parquet"
    if not documents_path.is_file() or not relations_path.is_file():
        raise ValueError("Lakehouse metadata is missing; run build_lakehouse_metadata.py first")

    connection = duckdb.connect(":memory:")
    try:
        document_rows = connection.execute(
            """
            SELECT
                document_id, sha256, size_bytes, file_extension, document_kind,
                raw_path, integrity_status
            FROM read_parquet(?)
            ORDER BY document_id
            """,
            [str(documents_path)],
        ).fetchall()
        relation_rows = connection.execute(
            """
            SELECT document_id, source_id, company_id, report_year, source_url
            FROM read_parquet(?)
            ORDER BY document_id, source_id, source_url
            """,
            [str(relations_path)],
        ).fetchall()
    finally:
        connection.close()

    relations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for document_id, source_id, company_id, report_year, source_url in relation_rows:
        relations[document_id].append(
            {
                "source_id": source_id,
                "company_id": company_id,
                "report_year": report_year,
                "source_url": source_url,
            }
        )

    return [
        {
            "document_id": row[0],
            "sha256": row[1],
            "size_bytes": row[2],
            "file_extension": row[3],
            "document_kind": row[4],
            "raw_path": row[5],
            "integrity_status": row[6],
            "relations": relations.get(row[0], []),
        }
        for row in document_rows
        if str(row[3] or "").casefold() in SUPPORTED_EXTENSIONS
    ]


class SilverBuilder:
    def __init__(
        self,
        lake_root: Path,
        matcher: AccountMatcher,
        worker_path: Path,
        worker_python: Path,
        runtime_fingerprint: str,
        run_id: str,
        started_at: datetime,
    ) -> None:
        self.lake_root = lake_root.resolve()
        self.matcher = matcher
        self.worker_path = worker_path.resolve()
        self.worker_python = worker_python.resolve()
        self.runtime_fingerprint = runtime_fingerprint
        self.code_sha256 = sha256_file(Path(__file__).resolve())
        self.run_id = run_id
        self.started_at = started_at
        self.content_units: list[dict[str, Any]] = []
        self.tables: list[dict[str, Any]] = []
        self.table_cells: list[dict[str, Any]] = []
        self.structured_values: list[dict[str, Any]] = []
        self.links: list[dict[str, Any]] = []
        self.quality: list[dict[str, Any]] = []
        self.extractions: list[dict[str, Any]] = []
        self.line_candidates: list[dict[str, Any]] = []
        self.fact_candidates: list[dict[str, Any]] = []

    def extraction_id(self, document_id: str) -> str:
        value = (
            f"{document_id}:{PIPELINE_VERSION}:{self.matcher.config_sha256}:"
            f"{self.code_sha256}:{sha256_file(self.worker_path)}:"
            f"{self.runtime_fingerprint}"
        )
        return sha256_text(value)

    def add_quality(
        self,
        *,
        extraction_id: str,
        attempt_id: str,
        document_id: str,
        severity: str,
        check_name: str,
        message: str,
        entity_type: str = "document",
        entity_id: str | None = None,
        expected_value: str | None = None,
        actual_value: str | None = None,
    ) -> None:
        entity_id = entity_id or document_id
        quality_id = sha256_text(
            f"{self.run_id}:{document_id}:{entity_type}:{entity_id}:{check_name}:{message}"
        )
        self.quality.append(
            {
                "quality_event_id": quality_id,
                "run_id": self.run_id,
                "attempt_id": attempt_id,
                "extraction_id": extraction_id,
                "document_id": document_id,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "severity": severity,
                "check_name": check_name,
                "expected_value": expected_value,
                "actual_value": actual_value,
                "message": message,
                "checked_at": self.started_at,
            }
        )

    def add_content_unit(
        self,
        *,
        extraction_id: str,
        document_id: str,
        unit_kind: str,
        ordinal: int,
        locator: dict[str, Any],
        raw_text: str,
        extraction_method: str,
        extraction_status: str = "succeeded",
        parent_unit_id: str | None = None,
        confidence_score: float | None = None,
        error_message: str | None = None,
    ) -> str:
        locator_json = json_dumps(locator)
        unit_id = sha256_text(f"{extraction_id}:{unit_kind}:{locator_json}")
        normalized_text = "\n".join(
            collapse_spaces(line) for line in clean_text(raw_text).splitlines() if collapse_spaces(line)
        )
        self.content_units.append(
            {
                "unit_id": unit_id,
                "extraction_id": extraction_id,
                "document_id": document_id,
                "parent_unit_id": parent_unit_id,
                "unit_kind": unit_kind,
                "ordinal": ordinal,
                "source_locator_json": locator_json,
                "extraction_status": extraction_status,
                "extraction_method": extraction_method,
                "raw_text": clean_text(raw_text),
                "normalized_text": normalized_text,
                "text_sha256": sha256_text(clean_text(raw_text)),
                "normalization_version": "whitespace-v1",
                "confidence_score": confidence_score,
                "error_message": error_message,
            }
        )
        return unit_id

    def add_link(
        self,
        *,
        extraction_id: str,
        document_id: str,
        locator: dict[str, Any],
        link_kind: str,
        raw_target: str,
        anchor_text: str | None,
    ) -> None:
        target = clean_text(raw_target)
        parsed = urlsplit(target)
        resolved = target if parsed.scheme in {"http", "https"} and parsed.hostname else None
        link_id = sha256_text(
            f"{extraction_id}:{link_kind}:{json_dumps(locator)}:{target}"
        )
        lowered = f"{target} {anchor_text or ''}".casefold()
        relationship = "unknown"
        if "fact" in lowered and "sheet" in lowered:
            relationship = "fact_sheet"
        elif re.search(r"financial|earning|result|report|실적|보고서", lowered):
            relationship = "report"
        elif re.search(r"conference|call|설명회", lowered):
            relationship = "conference_call"
        self.links.append(
            {
                "link_id": link_id,
                "extraction_id": extraction_id,
                "document_id": document_id,
                "source_locator_json": json_dumps(locator),
                "link_kind": link_kind,
                "raw_target": target,
                "resolved_url": resolved,
                "anchor_text": collapse_spaces(anchor_text or "") or None,
                "relationship_hint": relationship,
            }
        )

    def add_table(
        self,
        *,
        extraction_id: str,
        document_id: str,
        table_kind: str,
        ordinal: int,
        locator: dict[str, Any],
        caption_raw: str | None,
        row_count: int,
        column_count: int,
        extraction_method: str,
        confidence_score: float,
        structure_metadata: dict[str, Any] | None = None,
    ) -> str:
        locator_json = json_dumps(locator)
        table_id = sha256_text(f"{extraction_id}:{table_kind}:{locator_json}")
        self.tables.append(
            {
                "table_id": table_id,
                "extraction_id": extraction_id,
                "document_id": document_id,
                "table_kind": table_kind,
                "ordinal": ordinal,
                "source_locator_json": locator_json,
                "caption_raw": caption_raw,
                "row_count": row_count,
                "column_count": column_count,
                "extraction_method": extraction_method,
                "structure_status": "extracted",
                "confidence_score": confidence_score,
                "structure_metadata_json": json_dumps(structure_metadata or {}),
            }
        )
        return table_id

    def add_table_cell(
        self,
        *,
        table_id: str,
        extraction_id: str,
        document_id: str,
        row_index: int,
        column_index: int,
        locator: dict[str, Any],
        raw_text: str | None,
        source_value_type: str,
        raw_value_text: str | None,
        numeric_value: str | None = None,
        boolean_value: bool | None = None,
        date_value: str | None = None,
        formula_raw: str | None = None,
        formula_is_external: bool = False,
        cached_value_raw: str | None = None,
        number_format: str | None = None,
        style_id: int | None = None,
        row_span: int = 1,
        column_span: int = 1,
        merged_range: str | None = None,
        merge_anchor: str | None = None,
        is_hidden_row: bool = False,
        is_hidden_column: bool = False,
    ) -> str:
        locator_json = json_dumps(locator)
        cell_id = sha256_text(f"{table_id}:{row_index}:{column_index}:{locator_json}")
        raw_text = clean_text(raw_text) if raw_text is not None else None
        normalized_text = collapse_spaces(raw_text or "") or None
        cell_hash = sha256_text(
            json_dumps(
                {
                    "raw_text": raw_text,
                    "source_value_type": source_value_type,
                    "raw_value_text": raw_value_text,
                    "numeric_value": numeric_value,
                    "boolean_value": boolean_value,
                    "date_value": date_value,
                    "formula_raw": formula_raw,
                    "formula_is_external": formula_is_external,
                    "cached_value_raw": cached_value_raw,
                    "number_format": number_format,
                    "merged_range": merged_range,
                    "merge_anchor": merge_anchor,
                    "is_hidden_row": is_hidden_row,
                    "is_hidden_column": is_hidden_column,
                }
            )
        )
        self.table_cells.append(
            {
                "cell_id": cell_id,
                "table_id": table_id,
                "extraction_id": extraction_id,
                "document_id": document_id,
                "row_index": row_index,
                "column_index": column_index,
                "row_span": row_span,
                "column_span": column_span,
                "source_locator_json": locator_json,
                "raw_text": raw_text,
                "normalized_text": normalized_text,
                "source_value_type": source_value_type,
                "raw_value_text": raw_value_text,
                "numeric_value": numeric_value,
                "boolean_value": boolean_value,
                "date_value": date_value,
                "formula_raw": formula_raw,
                "formula_is_external": formula_is_external,
                "cached_value_raw": cached_value_raw,
                "number_format": number_format,
                "style_id": style_id,
                "merged_range": merged_range,
                "merge_anchor": merge_anchor,
                "is_hidden_row": is_hidden_row,
                "is_hidden_column": is_hidden_column,
                "cell_hash": cell_hash,
            }
        )
        return cell_id

    def run_worker(self, raw_path: Path, extension: str) -> list[dict[str, Any]]:
        process = subprocess.run(
            [
                str(self.worker_python),
                str(self.worker_path),
                "--path",
                str(raw_path),
                "--format",
                extension.removeprefix("."),
                "--ocr-languages",
                OCR_LANGUAGES,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=600,
            check=False,
        )
        if process.returncode:
            raise ValueError(
                f"Worker failed with exit {process.returncode}: {process.stderr.strip()[:1000]}"
            )
        records: list[dict[str, Any]] = []
        for line_number, line in enumerate(process.stdout.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Worker emitted invalid JSON on line {line_number}: {error}"
                ) from error
            if not isinstance(value, dict) or not value.get("record_type"):
                raise ValueError(f"Invalid worker record on line {line_number}")
            records.append(value)
        if not records:
            raise ValueError("Worker emitted no records")
        return records

    def extract_html(
        self,
        *,
        document_id: str,
        extraction_id: str,
        raw_path: Path,
    ) -> dict[str, Any]:
        payload = raw_path.read_bytes()
        decoded = payload.decode("utf-8", errors="replace")
        parser = HTMLContentParser()
        parser.feed(decoded)
        parser.close()
        text = parser.text()
        self.add_content_unit(
            extraction_id=extraction_id,
            document_id=document_id,
            unit_kind="html_document",
            ordinal=1,
            locator={"dom": "/html"},
            raw_text=text,
            extraction_method="stdlib_html_parser",
            extraction_status="succeeded" if text else "empty",
            confidence_score=0.9,
        )

        grouped_cells: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for cell in parser.table_cells:
            grouped_cells[int(cell["table_index"])].append(cell)
        for table_index, cells in sorted(grouped_cells.items()):
            row_count = max(int(cell["row_index"]) for cell in cells)
            column_count = max(int(cell["column_index"]) for cell in cells)
            first_row = " | ".join(
                cell["text"]
                for cell in sorted(cells, key=lambda item: item["column_index"])
                if int(cell["row_index"]) == 1 and cell["text"]
            )
            table_id = self.add_table(
                extraction_id=extraction_id,
                document_id=document_id,
                table_kind="html_table",
                ordinal=table_index,
                locator={"table_index": table_index},
                caption_raw=first_row[:1000] or None,
                row_count=row_count,
                column_count=column_count,
                extraction_method="stdlib_html_parser",
                confidence_score=0.95,
            )
            for cell in cells:
                numeric = parse_number_text(cell["text"])
                self.add_table_cell(
                    table_id=table_id,
                    extraction_id=extraction_id,
                    document_id=document_id,
                    row_index=int(cell["row_index"]),
                    column_index=int(cell["column_index"]),
                    locator={
                        "table_index": table_index,
                        "row": int(cell["row_index"]),
                        "column": int(cell["column_index"]),
                    },
                    raw_text=cell["text"],
                    source_value_type="number" if numeric is not None else "text",
                    raw_value_text=cell["text"],
                    numeric_value=numeric,
                    row_span=int(cell["rowspan"]),
                    column_span=int(cell["colspan"]),
                )

        for index, link in enumerate(parser.links, start=1):
            self.add_link(
                extraction_id=extraction_id,
                document_id=document_id,
                locator={"anchor_index": index},
                link_kind="html_anchor",
                raw_target=link["href"],
                anchor_text=link.get("link_text"),
            )
        return {
            "page_count": None,
            "sheet_count": None,
            "ocr_page_count": 0,
            "properties_json": json_dumps(
                {"encoding": "utf-8-with-replacement", "table_count": len(grouped_cells)}
            ),
            "worker_version": "stdlib",
        }

    def extract_json(
        self,
        *,
        document_id: str,
        extraction_id: str,
        raw_path: Path,
    ) -> dict[str, Any]:
        decoded = raw_path.read_text(encoding="utf-8", errors="replace")
        duplicate_keys: list[str] = []

        def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    duplicate_keys.append(key)
                result[key] = value
            return result

        payload = json.loads(decoded, object_pairs_hook=pairs_hook)
        canonical = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        self.add_content_unit(
            extraction_id=extraction_id,
            document_id=document_id,
            unit_kind="json_document",
            ordinal=1,
            locator={"json_pointer": "/"},
            raw_text=canonical,
            extraction_method="stdlib_json",
            confidence_score=1.0,
        )
        for ordinal, (pointer, value) in enumerate(flatten_json(payload), start=1):
            if isinstance(value, dict):
                fields = {
                    "value_kind": "object",
                    "value_text": json_dumps(value),
                    "numeric_value": None,
                    "boolean_value": None,
                    "date_value": None,
                }
            elif isinstance(value, list):
                fields = {
                    "value_kind": "array",
                    "value_text": json_dumps(value),
                    "numeric_value": None,
                    "boolean_value": None,
                    "date_value": None,
                }
            else:
                fields = scalar_fields(value)
            value_id = sha256_text(f"{extraction_id}:json:{pointer}")
            self.structured_values.append(
                {
                    "value_id": value_id,
                    "extraction_id": extraction_id,
                    "document_id": document_id,
                    "ordinal": ordinal,
                    "source_locator_json": json_dumps({"json_pointer": pointer}),
                    "value_path": pointer,
                    **fields,
                }
            )
            if isinstance(value, str):
                for url_index, url in enumerate(URL_PATTERN.findall(value), start=1):
                    self.add_link(
                        extraction_id=extraction_id,
                        document_id=document_id,
                        locator={"json_pointer": pointer, "url_index": url_index},
                        link_kind="json_url",
                        raw_target=url,
                        anchor_text=None,
                    )
        return {
            "page_count": None,
            "sheet_count": None,
            "ocr_page_count": 0,
            "properties_json": json_dumps(
                {
                    "root_type": type(payload).__name__,
                    "duplicate_keys": sorted(set(duplicate_keys)),
                }
            ),
            "worker_version": "stdlib",
            "duplicate_keys": sorted(set(duplicate_keys)),
        }

    def extract_worker_document(
        self,
        *,
        document_id: str,
        extraction_id: str,
        raw_path: Path,
        extension: str,
    ) -> dict[str, Any]:
        records = self.run_worker(raw_path, extension)
        headers = [record for record in records if record["record_type"] == "document"]
        if len(headers) != 1:
            raise ValueError(f"Worker must emit exactly one document record, got {len(headers)}")
        header = headers[0]
        if extension == ".pdf":
            for record in records:
                if record["record_type"] != "text_unit":
                    continue
                text = clean_text(record.get("text"))
                self.add_content_unit(
                    extraction_id=extraction_id,
                    document_id=document_id,
                    unit_kind="pdf_page",
                    ordinal=int(record["unit_index"]),
                    locator={"page": int(record["unit_index"])},
                    raw_text=text,
                    extraction_method=str(record["extraction_method"]),
                    extraction_status="succeeded" if text else "empty",
                    confidence_score=(
                        0.72
                        if record["extraction_method"] == "tesseract_ocr"
                        else 0.98
                    ),
                )
        else:
            sheet_records = {
                int(record["sheet_index"]): record
                for record in records
                if record["record_type"] == "sheet"
            }
            table_by_sheet: dict[int, str] = {}
            for sheet_index, record in sorted(sheet_records.items()):
                table_by_sheet[sheet_index] = self.add_table(
                    extraction_id=extraction_id,
                    document_id=document_id,
                    table_kind="xlsx_sheet_range",
                    ordinal=sheet_index,
                    locator={"sheet": record["sheet_name"], "range": "used_range"},
                    caption_raw=str(record["sheet_name"]),
                    row_count=int(record["max_row"]),
                    column_count=int(record["max_column"]),
                    extraction_method="openpyxl_read_only_source",
                    confidence_score=1.0,
                    structure_metadata={
                        key: record.get(key)
                        for key in (
                            "sheet_state",
                            "declared_range",
                            "actual_min_row",
                            "actual_max_row",
                            "actual_min_column",
                            "actual_max_column",
                            "nonempty_cell_count",
                            "formula_count",
                            "external_formula_count",
                            "comment_count",
                            "hidden_row_count",
                            "hidden_column_count",
                            "merged_ranges_json",
                        )
                    },
                )

            row_cells: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
            for record in records:
                if record["record_type"] != "cell":
                    continue
                sheet_index = int(record["sheet_index"])
                table_id = table_by_sheet.get(sheet_index)
                if not table_id:
                    raise ValueError(f"Cell references unknown sheet index {sheet_index}")
                self.add_table_cell(
                    table_id=table_id,
                    extraction_id=extraction_id,
                    document_id=document_id,
                    row_index=int(record["row_index"]),
                    column_index=int(record["column_index"]),
                    locator={
                        "sheet": record["sheet_name"],
                        "cell": record["cell_reference"],
                    },
                    raw_text=record.get("value_text"),
                    source_value_type=str(record.get("value_kind") or record.get("data_type")),
                    raw_value_text=record.get("value_text"),
                    numeric_value=record.get("numeric_value"),
                    boolean_value=record.get("boolean_value"),
                    date_value=record.get("date_value"),
                    formula_raw=record.get("formula"),
                    formula_is_external=bool(record.get("formula_is_external")),
                    cached_value_raw=record.get("cached_formula_value_text"),
                    number_format=record.get("number_format"),
                    style_id=record.get("style_id"),
                    merged_range=record.get("merged_range"),
                    merge_anchor=record.get("merge_anchor"),
                    is_hidden_row=bool(record.get("is_hidden_row")),
                    is_hidden_column=bool(record.get("is_hidden_column")),
                )
                row_cells[(sheet_index, int(record["row_index"]))].append(record)

            for ordinal, ((sheet_index, row_index), cells) in enumerate(
                sorted(row_cells.items()), start=1
            ):
                cells.sort(key=lambda item: int(item["column_index"]))
                row_text = "\t".join(
                    clean_text(
                        cell.get("value_text")
                        if cell.get("value_text") is not None
                        else cell.get("formula")
                    )
                    for cell in cells
                )
                self.add_content_unit(
                    extraction_id=extraction_id,
                    document_id=document_id,
                    unit_kind="xlsx_row",
                    ordinal=ordinal,
                    locator={
                        "sheet": cells[0]["sheet_name"],
                        "row": row_index,
                    },
                    raw_text=row_text,
                    extraction_method="openpyxl_cached_values",
                    confidence_score=1.0,
                )

        return {
            "page_count": header.get("page_count"),
            "sheet_count": header.get("sheet_count"),
            "ocr_page_count": int(header.get("ocr_pages") or 0),
            "properties_json": header.get("properties_json") or "{}",
            "worker_version": header.get("worker_version") or "unknown",
            "worker_issues": [
                record for record in records if record["record_type"] == "issue"
            ],
        }

    @staticmethod
    def period_token(cell: dict[str, Any]) -> str | None:
        value = collapse_spaces(cell.get("raw_value_text") or cell.get("raw_text") or "")
        if not value:
            return None
        numeric = parse_number_text(value)
        if numeric and re.fullmatch(r"(?:19|20)\d{2}", numeric):
            return numeric
        return value if PERIOD_PATTERN.fullmatch(value) else None

    def build_xlsx_fact_candidates(
        self,
        *,
        document_id: str,
        extraction_id: str,
        table_start: int,
        cell_start: int,
    ) -> None:
        tables = {
            row["table_id"]: row
            for row in self.tables[table_start:]
            if row["document_id"] == document_id
            and row["table_kind"] == "xlsx_sheet_range"
        }
        cells_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for cell in self.table_cells[cell_start:]:
            if cell["table_id"] in tables:
                cells_by_table[cell["table_id"]].append(cell)

        for table_id, cells in sorted(cells_by_table.items()):
            table = tables[table_id]
            locator = json.loads(table["source_locator_json"])
            sheet_name = str(locator.get("sheet") or table.get("caption_raw") or "")
            rows: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for cell in cells:
                rows[int(cell["row_index"])].append(cell)
            for row in rows.values():
                row.sort(key=lambda value: int(value["column_index"]))

            context = "\n".join(
                " ".join(cell.get("raw_text") or "" for cell in rows[row_index])
                for row_index in sorted(rows)[:4]
            )
            statement = statement_hint(sheet_name, context)
            scope = consolidation_hint(sheet_name, context)
            header_by_column: dict[int, str] = {}
            currency: str | None = None
            scale: int | None = None
            unit_raw: str | None = None

            for row_index in sorted(rows):
                row = rows[row_index]
                row_text = " ".join(cell.get("raw_text") or "" for cell in row)
                row_currency, row_scale = unit_hints(row_text)
                if row_currency or row_scale:
                    currency = row_currency or currency
                    scale = row_scale or scale
                    unit_raw = collapse_spaces(row_text)[:1000]

                period_cells = [
                    (int(cell["column_index"]), self.period_token(cell)) for cell in row
                ]
                period_cells = [value for value in period_cells if value[1]]
                if len(period_cells) >= 2:
                    header_by_column.update(
                        {column: str(period) for column, period in period_cells}
                    )

                label_cell = next(
                    (
                        cell
                        for cell in row
                        if cell.get("raw_text")
                        and cell.get("numeric_value") is None
                        and not self.period_token(cell)
                    ),
                    None,
                )
                if not label_cell:
                    continue
                label = collapse_spaces(label_cell["raw_text"])
                accounts = self.matcher.match(label)
                if not accounts:
                    continue
                label_column = int(label_cell["column_index"])
                numeric_cells = [
                    cell
                    for cell in row
                    if int(cell["column_index"]) > label_column
                    and cell.get("numeric_value") is not None
                ]
                for account in accounts:
                    for cell in numeric_cells:
                        column = int(cell["column_index"])
                        period_raw = header_by_column.get(column)
                        candidate_id = sha256_text(
                            f"{extraction_id}:xlsx_fact:{account['account_id']}:"
                            f"{sheet_name}:{cell['row_index']}:{column}"
                        )
                        confidence = 0.55
                        if period_raw:
                            confidence += 0.15
                        if currency:
                            confidence += 0.1
                        if statement:
                            confidence += 0.05
                        self.fact_candidates.append(
                            {
                                "candidate_id": candidate_id,
                                "extraction_id": extraction_id,
                                "document_id": document_id,
                                "evidence_unit_id": None,
                                "evidence_cell_id": cell["cell_id"],
                                "source_locator_json": cell["source_locator_json"],
                                "label_raw": label,
                                "value_raw": cell.get("raw_value_text"),
                                "numeric_value": cell.get("numeric_value"),
                                "formula_raw": cell.get("formula_raw"),
                                "unit_raw": unit_raw,
                                "period_raw": period_raw,
                                "currency_raw": currency,
                                "unit_scale_multiplier": scale,
                                "scope_raw": scope,
                                "statement_type_hint": statement,
                                "proposed_account_id": account["account_id"],
                                "proposed_account_name": account["canonical_name"],
                                "candidate_method": "xlsx_label_period_header_v1",
                                "method_version": PIPELINE_VERSION,
                                "mapping_version": self.matcher.mapping_version,
                                "confidence_score": min(confidence, 0.9),
                                "evidence_class": "reported_source_candidate",
                                "review_status": "unreviewed",
                                "reviewed_by": None,
                                "reviewed_at": None,
                            }
                        )

    def build_line_candidates(
        self,
        *,
        document_id: str,
        extraction_id: str,
        unit_start: int,
    ) -> None:
        for unit in self.content_units[unit_start:]:
            if unit["document_id"] != document_id or unit["unit_kind"] == "xlsx_row":
                continue
            for line_number, line in enumerate(unit["normalized_text"].splitlines(), start=1):
                tokens = [collapse_spaces(value) for value in NUMBER_TOKEN_PATTERN.findall(line)]
                if not tokens:
                    continue
                first_match = NUMBER_TOKEN_PATTERN.search(line)
                if not first_match:
                    continue
                label = line[: first_match.start()].strip(" :-–—|\t")
                label = re.sub(
                    r"\((?:krw|jpy|usd|eur|mn|bn|million|billion|억원|백만원)[^)]*\)",
                    "",
                    label,
                    flags=re.IGNORECASE,
                ).strip()
                accounts = self.matcher.match(label)
                if not accounts:
                    continue
                periods = [
                    value
                    for value in (collapse_spaces(token) for token in re.findall(r"\b[\w ]+\b", line))
                    if PERIOD_PATTERN.fullmatch(value)
                ]
                currency, scale = unit_hints(line)
                locator = json.loads(unit["source_locator_json"])
                locator["line"] = line_number
                for account in accounts:
                    candidate_id = sha256_text(
                        f"{extraction_id}:line:{unit['unit_id']}:{line_number}:"
                        f"{account['account_id']}"
                    )
                    method = (
                        "ocr_account_line_v1"
                        if "ocr" in unit["extraction_method"]
                        else "native_account_line_v1"
                    )
                    self.line_candidates.append(
                        {
                            "candidate_id": candidate_id,
                            "extraction_id": extraction_id,
                            "document_id": document_id,
                            "evidence_unit_id": unit["unit_id"],
                            "source_locator_json": json_dumps(locator),
                            "label_raw": label,
                            "evidence_text": line[:4000],
                            "numeric_tokens_json": json_dumps(tokens),
                            "period_raw": periods[0] if periods else None,
                            "currency_raw": currency,
                            "unit_scale_multiplier": scale,
                            "proposed_account_id": account["account_id"],
                            "proposed_account_name": account["canonical_name"],
                            "candidate_method": method,
                            "method_version": PIPELINE_VERSION,
                            "mapping_version": self.matcher.mapping_version,
                            "confidence_score": 0.3 if "ocr" in method else 0.5,
                            "evidence_class": "reported_source_line_candidate",
                            "review_status": "unreviewed",
                        }
                    )

    def document_output_hash(self, document_id: str) -> str:
        collections: Sequence[tuple[str, list[dict[str, Any]], str]] = (
            ("units", self.content_units, "unit_id"),
            ("tables", self.tables, "table_id"),
            ("cells", self.table_cells, "cell_id"),
            ("structured", self.structured_values, "value_id"),
            ("links", self.links, "link_id"),
            ("line_candidates", self.line_candidates, "candidate_id"),
            ("fact_candidates", self.fact_candidates, "candidate_id"),
        )
        payload: dict[str, Any] = {}
        for name, rows, identifier in collections:
            payload[name] = sorted(
                (row for row in rows if row["document_id"] == document_id),
                key=lambda row: row[identifier],
            )
        return sha256_text(json_dumps(payload))

    def extract_document(self, document: dict[str, Any]) -> None:
        document_id = str(document["document_id"])
        extraction_id = self.extraction_id(document_id)
        attempt_id = sha256_text(f"{self.run_id}:{extraction_id}")
        started_at = utc_now()
        extension = str(document["file_extension"] or "").casefold()
        raw_path = safe_raw_path(self.lake_root, str(document["raw_path"]))
        unit_start = len(self.content_units)
        table_start = len(self.tables)
        cell_start = len(self.table_cells)
        structured_start = len(self.structured_values)
        link_start = len(self.links)
        line_candidate_start = len(self.line_candidates)
        fact_candidate_start = len(self.fact_candidates)
        quality_start = len(self.quality)
        error_class: str | None = None
        error_message: str | None = None
        result: dict[str, Any] = {
            "page_count": None,
            "sheet_count": None,
            "ocr_page_count": 0,
            "properties_json": "{}",
            "worker_version": "none",
        }
        status = "succeeded"

        try:
            if not SHA256_PATTERN.fullmatch(document_id) or document_id != document["sha256"]:
                raise ValueError(f"Invalid document checksum identity: {document_id}")
            if not raw_path.is_file():
                raise ValueError(f"Raw object is missing: {raw_path}")
            if raw_path.stat().st_size != int(document["size_bytes"]):
                raise ValueError(f"Raw object size mismatch: {raw_path}")
            actual_sha256 = sha256_file(raw_path)
            if actual_sha256 != document_id:
                raise ValueError(f"Raw object checksum mismatch: {raw_path}")
            if document["integrity_status"] != "valid":
                raise ValueError(
                    f"Metadata integrity_status is {document['integrity_status']!r}"
                )

            if extension not in SUPPORTED_EXTENSIONS:
                status = "unsupported"
            elif extension == ".html":
                result = self.extract_html(
                    document_id=document_id,
                    extraction_id=extraction_id,
                    raw_path=raw_path,
                )
            elif extension == ".json":
                result = self.extract_json(
                    document_id=document_id,
                    extraction_id=extraction_id,
                    raw_path=raw_path,
                )
                if result.get("duplicate_keys"):
                    self.add_quality(
                        extraction_id=extraction_id,
                        attempt_id=attempt_id,
                        document_id=document_id,
                        severity="warning",
                        check_name="json_duplicate_keys",
                        message="Duplicate JSON object keys were encountered.",
                        actual_value=json_dumps(result["duplicate_keys"]),
                    )
            else:
                result = self.extract_worker_document(
                    document_id=document_id,
                    extraction_id=extraction_id,
                    raw_path=raw_path,
                    extension=extension,
                )
                for issue in result.get("worker_issues", []):
                    severity = str(issue.get("severity") or "warning")
                    self.add_quality(
                        extraction_id=extraction_id,
                        attempt_id=attempt_id,
                        document_id=document_id,
                        severity=severity,
                        check_name=str(issue.get("check_name") or "worker_issue"),
                        message=str(issue.get("message") or "Worker reported an issue."),
                        entity_type="content_unit",
                        entity_id=(
                            f"{document_id}:{issue.get('unit_index')}"
                            if issue.get("unit_index") is not None
                            else document_id
                        ),
                    )
                    if severity == "error":
                        status = "partial"

            if status in {"succeeded", "partial"}:
                if extension == ".xlsx":
                    self.build_xlsx_fact_candidates(
                        document_id=document_id,
                        extraction_id=extraction_id,
                        table_start=table_start,
                        cell_start=cell_start,
                    )
                self.build_line_candidates(
                    document_id=document_id,
                    extraction_id=extraction_id,
                    unit_start=unit_start,
                )

                units = self.content_units[unit_start:]
                text_chars = sum(len(row["raw_text"] or "") for row in units)
                if not units and not self.table_cells[cell_start:]:
                    self.add_quality(
                        extraction_id=extraction_id,
                        attempt_id=attempt_id,
                        document_id=document_id,
                        severity="error",
                        check_name="nonempty_extraction",
                        message="No content units or table cells were extracted.",
                    )
                    status = "partial"
                if extension == ".pdf":
                    expected = int(result.get("page_count") or 0)
                    actual = sum(row["unit_kind"] == "pdf_page" for row in units)
                    if expected != actual:
                        self.add_quality(
                            extraction_id=extraction_id,
                            attempt_id=attempt_id,
                            document_id=document_id,
                            severity="error",
                            check_name="pdf_page_count",
                            message="PDF page count does not match extracted page outcomes.",
                            expected_value=str(expected),
                            actual_value=str(actual),
                        )
                        status = "partial"
                    if text_chars < max(expected, 1) * 20:
                        self.add_quality(
                            extraction_id=extraction_id,
                            attempt_id=attempt_id,
                            document_id=document_id,
                            severity="warning",
                            check_name="pdf_low_text_volume",
                            message="PDF has unusually little extracted text after OCR fallback.",
                            actual_value=str(text_chars),
                        )
                if extension == ".xlsx":
                    expected = int(result.get("sheet_count") or 0)
                    actual = len(self.tables[table_start:])
                    if expected != actual:
                        self.add_quality(
                            extraction_id=extraction_id,
                            attempt_id=attempt_id,
                            document_id=document_id,
                            severity="error",
                            check_name="xlsx_sheet_count",
                            message="Workbook sheet count does not match extracted sheet tables.",
                            expected_value=str(expected),
                            actual_value=str(actual),
                        )
                        status = "partial"
        except Exception as error:
            # A failed document must not leak partially emitted rows into the
            # canonical snapshot. Partial status is reserved for extractors
            # that returned a structurally valid, explicitly incomplete result.
            del self.content_units[unit_start:]
            del self.tables[table_start:]
            del self.table_cells[cell_start:]
            del self.structured_values[structured_start:]
            del self.links[link_start:]
            del self.line_candidates[line_candidate_start:]
            del self.fact_candidates[fact_candidate_start:]
            del self.quality[quality_start:]
            status = "failed"
            error_class = type(error).__name__
            error_message = str(error)
            self.add_quality(
                extraction_id=extraction_id,
                attempt_id=attempt_id,
                document_id=document_id,
                severity="error",
                check_name="document_extraction",
                message=f"{error_class}: {error_message}",
            )

        units = self.content_units[unit_start:]
        doc_text = "\n".join(row["normalized_text"] for row in units)
        filename = Path(str(document["raw_path"])).name.split("__", 1)[-1]
        output_hash = self.document_output_hash(document_id)
        warnings = sum(
            row["severity"] == "warning" for row in self.quality[quality_start:]
        )
        errors = sum(row["severity"] == "error" for row in self.quality[quality_start:])
        if errors and status == "succeeded":
            status = "partial"
        finished_at = utc_now()
        self.extractions.append(
            {
                "attempt_id": attempt_id,
                "extraction_id": extraction_id,
                "run_id": self.run_id,
                "document_id": document_id,
                "input_sha256": document["sha256"],
                "raw_path": document["raw_path"],
                "file_format": extension.removeprefix("."),
                "document_kind": document["document_kind"],
                "extractor_name": (
                    "pypdf_openpyxl_worker"
                    if extension in {".pdf", ".xlsx"}
                    else "python_stdlib"
                ),
                "extractor_version": str(result.get("worker_version") or "unknown"),
                "pipeline_version": PIPELINE_VERSION,
                "config_sha256": self.matcher.config_sha256,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "classification": classify_document(extension, filename, doc_text),
                "language_hint": detect_language(doc_text),
                "page_count": result.get("page_count"),
                "sheet_count": result.get("sheet_count"),
                "unit_count": len(units),
                "table_count": len(self.tables) - table_start,
                "cell_count": len(self.table_cells) - cell_start,
                "structured_value_count": len(self.structured_values) - structured_start,
                "link_count": len(self.links) - link_start,
                "text_char_count": sum(len(row["raw_text"] or "") for row in units),
                "line_candidate_count": len(self.line_candidates) - line_candidate_start,
                "fact_candidate_count": len(self.fact_candidates) - fact_candidate_start,
                "ocr_page_count": result.get("ocr_page_count") or 0,
                "output_content_sha256": output_hash,
                "properties_json": result.get("properties_json") or "{}",
                "warning_count": warnings,
                "error_count": errors,
                "error_class": error_class,
                "error_message": error_message,
            }
        )


RUN_SCHEMA = (
    ("run_id", "VARCHAR"),
    ("pipeline_name", "VARCHAR"),
    ("pipeline_version", "VARCHAR"),
    ("schema_version", "INTEGER"),
    ("config_sha256", "VARCHAR"),
    ("code_revision", "VARCHAR"),
    ("started_at", "TIMESTAMPTZ"),
    ("finished_at", "TIMESTAMPTZ"),
    ("status", "VARCHAR"),
    ("requested_document_count", "INTEGER"),
    ("succeeded_count", "INTEGER"),
    ("partial_count", "INTEGER"),
    ("failed_count", "INTEGER"),
    ("unsupported_count", "INTEGER"),
    ("manifest_path", "VARCHAR"),
    ("manifest_sha256", "VARCHAR"),
    ("dataset_hashes_json", "JSON"),
)

EXTRACTION_SCHEMA = (
    ("attempt_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("run_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("input_sha256", "VARCHAR"),
    ("raw_path", "VARCHAR"),
    ("file_format", "VARCHAR"),
    ("document_kind", "VARCHAR"),
    ("extractor_name", "VARCHAR"),
    ("extractor_version", "VARCHAR"),
    ("pipeline_version", "VARCHAR"),
    ("config_sha256", "VARCHAR"),
    ("started_at", "TIMESTAMPTZ"),
    ("finished_at", "TIMESTAMPTZ"),
    ("status", "VARCHAR"),
    ("classification", "VARCHAR"),
    ("language_hint", "VARCHAR"),
    ("page_count", "INTEGER"),
    ("sheet_count", "INTEGER"),
    ("unit_count", "INTEGER"),
    ("table_count", "INTEGER"),
    ("cell_count", "BIGINT"),
    ("structured_value_count", "BIGINT"),
    ("link_count", "BIGINT"),
    ("text_char_count", "BIGINT"),
    ("line_candidate_count", "BIGINT"),
    ("fact_candidate_count", "BIGINT"),
    ("ocr_page_count", "INTEGER"),
    ("output_content_sha256", "VARCHAR"),
    ("properties_json", "JSON"),
    ("warning_count", "INTEGER"),
    ("error_count", "INTEGER"),
    ("error_class", "VARCHAR"),
    ("error_message", "VARCHAR"),
)

CONTENT_SCHEMA = (
    ("unit_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("parent_unit_id", "VARCHAR"),
    ("unit_kind", "VARCHAR"),
    ("ordinal", "BIGINT"),
    ("source_locator_json", "JSON"),
    ("extraction_status", "VARCHAR"),
    ("extraction_method", "VARCHAR"),
    ("raw_text", "VARCHAR"),
    ("normalized_text", "VARCHAR"),
    ("text_sha256", "VARCHAR"),
    ("normalization_version", "VARCHAR"),
    ("confidence_score", "DOUBLE"),
    ("error_message", "VARCHAR"),
)

TABLE_SCHEMA = (
    ("table_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("table_kind", "VARCHAR"),
    ("ordinal", "INTEGER"),
    ("source_locator_json", "JSON"),
    ("caption_raw", "VARCHAR"),
    ("row_count", "INTEGER"),
    ("column_count", "INTEGER"),
    ("extraction_method", "VARCHAR"),
    ("structure_status", "VARCHAR"),
    ("confidence_score", "DOUBLE"),
    ("structure_metadata_json", "JSON"),
)

CELL_SCHEMA = (
    ("cell_id", "VARCHAR"),
    ("table_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("row_index", "INTEGER"),
    ("column_index", "INTEGER"),
    ("row_span", "INTEGER"),
    ("column_span", "INTEGER"),
    ("source_locator_json", "JSON"),
    ("raw_text", "VARCHAR"),
    ("normalized_text", "VARCHAR"),
    ("source_value_type", "VARCHAR"),
    ("raw_value_text", "VARCHAR"),
    ("numeric_value", "DECIMAL(38, 12)"),
    ("boolean_value", "BOOLEAN"),
    ("date_value", "DATE"),
    ("formula_raw", "VARCHAR"),
    ("formula_is_external", "BOOLEAN"),
    ("cached_value_raw", "VARCHAR"),
    ("number_format", "VARCHAR"),
    ("style_id", "INTEGER"),
    ("merged_range", "VARCHAR"),
    ("merge_anchor", "VARCHAR"),
    ("is_hidden_row", "BOOLEAN"),
    ("is_hidden_column", "BOOLEAN"),
    ("cell_hash", "VARCHAR"),
)

STRUCTURED_SCHEMA = (
    ("value_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("ordinal", "BIGINT"),
    ("source_locator_json", "JSON"),
    ("value_path", "VARCHAR"),
    ("value_kind", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("numeric_value", "DECIMAL(38, 12)"),
    ("boolean_value", "BOOLEAN"),
    ("date_value", "DATE"),
)

LINK_SCHEMA = (
    ("link_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("source_locator_json", "JSON"),
    ("link_kind", "VARCHAR"),
    ("raw_target", "VARCHAR"),
    ("resolved_url", "VARCHAR"),
    ("anchor_text", "VARCHAR"),
    ("relationship_hint", "VARCHAR"),
)

LINE_CANDIDATE_SCHEMA = (
    ("candidate_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("evidence_unit_id", "VARCHAR"),
    ("source_locator_json", "JSON"),
    ("label_raw", "VARCHAR"),
    ("evidence_text", "VARCHAR"),
    ("numeric_tokens_json", "JSON"),
    ("period_raw", "VARCHAR"),
    ("currency_raw", "VARCHAR"),
    ("unit_scale_multiplier", "BIGINT"),
    ("proposed_account_id", "VARCHAR"),
    ("proposed_account_name", "VARCHAR"),
    ("candidate_method", "VARCHAR"),
    ("method_version", "VARCHAR"),
    ("mapping_version", "VARCHAR"),
    ("confidence_score", "DOUBLE"),
    ("evidence_class", "VARCHAR"),
    ("review_status", "VARCHAR"),
)

FACT_CANDIDATE_SCHEMA = (
    ("candidate_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("evidence_unit_id", "VARCHAR"),
    ("evidence_cell_id", "VARCHAR"),
    ("source_locator_json", "JSON"),
    ("label_raw", "VARCHAR"),
    ("value_raw", "VARCHAR"),
    ("numeric_value", "DECIMAL(38, 12)"),
    ("formula_raw", "VARCHAR"),
    ("unit_raw", "VARCHAR"),
    ("period_raw", "VARCHAR"),
    ("currency_raw", "VARCHAR"),
    ("unit_scale_multiplier", "BIGINT"),
    ("scope_raw", "VARCHAR"),
    ("statement_type_hint", "VARCHAR"),
    ("proposed_account_id", "VARCHAR"),
    ("proposed_account_name", "VARCHAR"),
    ("candidate_method", "VARCHAR"),
    ("method_version", "VARCHAR"),
    ("mapping_version", "VARCHAR"),
    ("confidence_score", "DOUBLE"),
    ("evidence_class", "VARCHAR"),
    ("review_status", "VARCHAR"),
    ("reviewed_by", "VARCHAR"),
    ("reviewed_at", "TIMESTAMPTZ"),
)

ACCOUNT_ALIAS_SCHEMA = (
    ("mapping_version", "VARCHAR"),
    ("account_id", "VARCHAR"),
    ("canonical_name", "VARCHAR"),
    ("pattern", "VARCHAR"),
    ("candidate_only", "BOOLEAN"),
)

QUALITY_SCHEMA = (
    ("quality_event_id", "VARCHAR"),
    ("run_id", "VARCHAR"),
    ("attempt_id", "VARCHAR"),
    ("extraction_id", "VARCHAR"),
    ("document_id", "VARCHAR"),
    ("entity_type", "VARCHAR"),
    ("entity_id", "VARCHAR"),
    ("severity", "VARCHAR"),
    ("check_name", "VARCHAR"),
    ("expected_value", "VARCHAR"),
    ("actual_value", "VARCHAR"),
    ("message", "VARCHAR"),
    ("checked_at", "TIMESTAMPTZ"),
)


def dict_rows(rows: Sequence[dict[str, Any]], schema: Sequence[tuple[str, str]]) -> list[tuple[Any, ...]]:
    names = [name for name, _ in schema]
    return [tuple(row.get(name) for name in names) for row in rows]


def read_existing_rows(
    path: Path, schema: Sequence[tuple[str, str]]
) -> list[tuple[Any, ...]]:
    if not path.is_file():
        return []
    names = [name for name, _ in schema]
    selected = ", ".join(f'"{name}"' for name in names)
    connection = duckdb.connect(":memory:")
    try:
        return connection.execute(
            f"SELECT {selected} FROM read_parquet(?)", [str(path)]
        ).fetchall()
    finally:
        connection.close()


def active_dataset_path(silver_root: Path, filename: str) -> Path:
    """Resolve history from the active immutable snapshot or legacy flat layout."""
    current = silver_root / "CURRENT"
    if current.is_symlink():
        snapshot = current.resolve(strict=True)
        snapshots_root = (silver_root / "snapshots").resolve(strict=False)
        if snapshots_root not in snapshot.parents:
            raise ValueError(f"Silver CURRENT leaves snapshots/: {snapshot}")
        return snapshot / filename
    if current.exists():
        raise ValueError("Silver CURRENT must be a symbolic link")
    return silver_root / filename


def read_history_rows(
    silver_root: Path,
    filename: str,
    schema: Sequence[tuple[str, str]],
) -> list[tuple[Any, ...]]:
    """Merge published and diagnostic history without duplicating prior rows."""
    candidates = [active_dataset_path(silver_root, filename)]
    diagnostic_root = silver_root / "diagnostic_snapshots"
    if diagnostic_root.is_dir():
        candidates.extend(sorted(diagnostic_root.glob(f"*/{filename}")))
    by_identifier: dict[Any, tuple[Any, ...]] = {}
    for candidate in candidates:
        for row in read_existing_rows(candidate, schema):
            by_identifier.setdefault(row[0], row)
    return [by_identifier[key] for key in sorted(by_identifier, key=str)]


def publish_current_snapshot(silver_root: Path, snapshot: Path) -> None:
    """Atomically switch readers after a complete snapshot has been published."""
    snapshots_root = (silver_root / "snapshots").resolve(strict=False)
    resolved_snapshot = snapshot.resolve(strict=True)
    if snapshots_root not in resolved_snapshot.parents:
        raise ValueError(f"Snapshot leaves Silver snapshots/: {resolved_snapshot}")
    relative_target = resolved_snapshot.relative_to(silver_root.resolve())
    temporary = silver_root / f".CURRENT.{uuid.uuid4().hex}.tmp"
    try:
        temporary.symlink_to(relative_target, target_is_directory=True)
        os.replace(temporary, silver_root / "CURRENT")
    finally:
        if temporary.is_symlink():
            temporary.unlink()


def ensure_unique(rows: Sequence[dict[str, Any]], identifier: str, label: str) -> None:
    values = [row[identifier] for row in rows]
    if len(values) != len(set(values)):
        raise ValueError(f"Duplicate {label} identifiers detected")


def logical_hash(rows: Sequence[dict[str, Any]], identifier: str) -> str:
    ordered = sorted(rows, key=lambda row: row[identifier])
    return sha256_text(json_dumps(ordered))


def validate_outputs(builder: SilverBuilder, inventory: Sequence[dict[str, Any]]) -> None:
    if len(builder.extractions) != len(inventory):
        raise ValueError("Every input document must have exactly one extraction outcome")
    ensure_unique(builder.extractions, "attempt_id", "attempt")
    ensure_unique(builder.extractions, "extraction_id", "extraction")
    ensure_unique(builder.content_units, "unit_id", "content unit")
    ensure_unique(builder.tables, "table_id", "table")
    ensure_unique(builder.table_cells, "cell_id", "table cell")
    ensure_unique(builder.structured_values, "value_id", "structured value")
    ensure_unique(builder.links, "link_id", "link")
    ensure_unique(builder.line_candidates, "candidate_id", "line candidate")
    ensure_unique(builder.fact_candidates, "candidate_id", "fact candidate")

    document_ids = {row["document_id"] for row in inventory}
    extraction_owners = {
        row["extraction_id"]: row["document_id"] for row in builder.extractions
    }
    table_owners = {
        row["table_id"]: (row["extraction_id"], row["document_id"])
        for row in builder.tables
    }
    unit_owners = {
        row["unit_id"]: (row["extraction_id"], row["document_id"])
        for row in builder.content_units
    }
    cell_owners = {
        row["cell_id"]: (row["extraction_id"], row["document_id"])
        for row in builder.table_cells
    }
    for collection_name, rows in (
        ("content unit", builder.content_units),
        ("table", builder.tables),
        ("table cell", builder.table_cells),
        ("structured value", builder.structured_values),
        ("link", builder.links),
        ("line candidate", builder.line_candidates),
        ("fact candidate", builder.fact_candidates),
    ):
        if any(row["document_id"] not in document_ids for row in rows):
            raise ValueError(f"{collection_name} references an unknown document")
        if any(
            extraction_owners.get(row["extraction_id"]) != row["document_id"]
            for row in rows
        ):
            raise ValueError(f"{collection_name} has mismatched extraction ownership")
    if any(
        table_owners.get(row["table_id"])
        != (row["extraction_id"], row["document_id"])
        for row in builder.table_cells
    ):
        raise ValueError("Table cell has mismatched table ownership")
    if any(
        unit_owners.get(row["evidence_unit_id"])
        != (row["extraction_id"], row["document_id"])
        for row in builder.line_candidates
    ):
        raise ValueError("Line candidate has mismatched evidence ownership")
    if any(
        cell_owners.get(row["evidence_cell_id"])
        != (row["extraction_id"], row["document_id"])
        for row in builder.fact_candidates
    ):
        raise ValueError("Fact candidate has mismatched evidence ownership")
    if any(
        row["parent_unit_id"] is not None
        and unit_owners.get(row["parent_unit_id"])
        != (row["extraction_id"], row["document_id"])
        for row in builder.content_units
    ):
        raise ValueError("Content unit has mismatched parent ownership")
    if any(row["review_status"] != "unreviewed" for row in builder.line_candidates):
        raise ValueError("Line candidates must start unreviewed")
    if any(row["review_status"] != "unreviewed" for row in builder.fact_candidates):
        raise ValueError("Fact candidates must start unreviewed")
    empty_output_extractions = {
        row["extraction_id"]
        for row in builder.extractions
        if row["status"] in {"failed", "unsupported"}
    }
    for collection_name, rows in (
        ("content unit", builder.content_units),
        ("table", builder.tables),
        ("table cell", builder.table_cells),
        ("structured value", builder.structured_values),
        ("link", builder.links),
        ("line candidate", builder.line_candidates),
        ("fact candidate", builder.fact_candidates),
    ):
        if any(row["extraction_id"] in empty_output_extractions for row in rows):
            raise ValueError(
                f"Failed or unsupported extraction emitted a {collection_name}"
            )


def _build_silver_locked(
    lake_root: Path,
    account_aliases_path: Path,
    worker_path: Path,
    explicit_worker_python: Path | None,
) -> dict[str, Any]:
    lake_root = lake_root.resolve()
    silver_root = lake_root / "silver"
    silver_root.mkdir(parents=True, exist_ok=True)
    inventory = load_inventory(lake_root)
    matcher = AccountMatcher(account_aliases_path)
    needs_worker = any(
        str(document["file_extension"]).casefold() in {".pdf", ".xlsx"}
        for document in inventory
    )
    worker_python = (
        select_worker_python(explicit_worker_python)
        if needs_worker
        else Path(sys.executable)
    )
    if not worker_path.is_file():
        raise ValueError(f"Extraction worker is missing: {worker_path}")
    runtime_environment, runtime_fingerprint = worker_environment(
        worker_python, worker_path
    )

    started_at = utc_now()
    run_id = f"{started_at.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    builder = SilverBuilder(
        lake_root,
        matcher,
        worker_path,
        worker_python,
        runtime_fingerprint,
        run_id,
        started_at,
    )
    for document in inventory:
        builder.extract_document(document)
    validate_outputs(builder, inventory)
    finished_at = utc_now()

    status_counts: dict[str, int] = defaultdict(int)
    for extraction in builder.extractions:
        status_counts[extraction["status"]] += 1
    if status_counts["failed"] == len(inventory) and inventory:
        run_status = "failed"
    elif (
        status_counts["failed"]
        or status_counts["partial"]
        or status_counts["unsupported"]
    ):
        run_status = "partial"
    else:
        run_status = "succeeded"

    canonical_datasets: dict[
        str, tuple[Sequence[tuple[str, str]], list[dict[str, Any]], str]
    ] = {
        "content_units": (CONTENT_SCHEMA, builder.content_units, "unit_id"),
        "tables": (TABLE_SCHEMA, builder.tables, "table_id"),
        "table_cells": (CELL_SCHEMA, builder.table_cells, "cell_id"),
        "structured_values": (
            STRUCTURED_SCHEMA,
            builder.structured_values,
            "value_id",
        ),
        "discovered_links": (LINK_SCHEMA, builder.links, "link_id"),
        "reported_line_candidates": (
            LINE_CANDIDATE_SCHEMA,
            builder.line_candidates,
            "candidate_id",
        ),
        "reported_fact_candidates": (
            FACT_CANDIDATE_SCHEMA,
            builder.fact_candidates,
            "candidate_id",
        ),
    }
    dataset_hashes = {
        name: logical_hash(rows, identifier)
        for name, (_, rows, identifier) in canonical_datasets.items()
    }
    dataset_hashes["account_aliases"] = sha256_text(
        json_dumps(matcher.parquet_rows())
    )
    dataset_hashes["current_document_extractions"] = logical_hash(
        builder.extractions, "attempt_id"
    )

    manifest_relative = (
        Path("silver")
        / "manifests_json"
        / started_at.strftime("%Y/%m/%d")
        / f"{run_id}.json"
    )
    published_to_current = run_status == "succeeded"
    snapshot_bucket = "snapshots" if published_to_current else "diagnostic_snapshots"
    snapshot_relative = Path("silver") / snapshot_bucket / run_id
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "pipeline_name": "game_accounting_silver_extraction",
        "pipeline_version": PIPELINE_VERSION,
        "started_at": isoformat(started_at),
        "finished_at": isoformat(finished_at),
        "status": run_status,
        "worker_python": str(worker_python),
        "worker_path": str(worker_path.resolve()),
        "config_path": str(account_aliases_path.resolve()),
        "config_sha256": matcher.config_sha256,
        "runtime_fingerprint": runtime_fingerprint,
        "runtime_environment": runtime_environment,
        "requested_document_count": len(inventory),
        "status_counts": dict(sorted(status_counts.items())),
        "row_counts": {
            "content_units": len(builder.content_units),
            "tables": len(builder.tables),
            "table_cells": len(builder.table_cells),
            "structured_values": len(builder.structured_values),
            "discovered_links": len(builder.links),
            "reported_line_candidates": len(builder.line_candidates),
            "reported_fact_candidates": len(builder.fact_candidates),
            "data_quality_events": len(builder.quality),
        },
        "dataset_hashes": dataset_hashes,
        "snapshot_path": snapshot_relative.as_posix(),
        "published_to_current": published_to_current,
    }

    staging = silver_root / ".staging" / run_id
    staging.mkdir(parents=True, exist_ok=False)
    staging_manifest = staging / "manifest.json"
    atomic_json_write(staging_manifest, manifest)
    manifest_sha256 = sha256_file(staging_manifest)
    code_revision = "sha256:" + sha256_text(
        f"{sha256_file(Path(__file__).resolve())}:{sha256_file(worker_path.resolve())}"
    )
    run_row = (
        run_id,
        "game_accounting_silver_extraction",
        PIPELINE_VERSION,
        1,
        matcher.config_sha256,
        code_revision,
        started_at,
        finished_at,
        run_status,
        len(inventory),
        status_counts["succeeded"],
        status_counts["partial"],
        status_counts["failed"],
        status_counts["unsupported"],
        manifest_relative.as_posix(),
        manifest_sha256,
        json_dumps(dataset_hashes),
    )

    histories = {
        "extraction_runs": (
            RUN_SCHEMA,
            read_history_rows(
                silver_root,
                "extraction_runs.parquet",
                RUN_SCHEMA,
            )
            + [run_row],
        ),
        "document_extractions": (
            EXTRACTION_SCHEMA,
            read_history_rows(
                silver_root,
                "document_extractions.parquet",
                EXTRACTION_SCHEMA,
            )
            + dict_rows(builder.extractions, EXTRACTION_SCHEMA),
        ),
        "data_quality_log": (
            QUALITY_SCHEMA,
            read_history_rows(
                silver_root,
                "data_quality_log.parquet",
                QUALITY_SCHEMA,
            )
            + dict_rows(builder.quality, QUALITY_SCHEMA),
        ),
    }

    connection = duckdb.connect(":memory:")
    try:
        for name, (schema, rows, identifier) in canonical_datasets.items():
            ordered = sorted(rows, key=lambda row: row[identifier])
            write_table(
                connection,
                name,
                schema,
                dict_rows(ordered, schema),
                staging / f"{name}.parquet",
            )
        write_table(
            connection,
            "account_aliases",
            ACCOUNT_ALIAS_SCHEMA,
            matcher.parquet_rows(),
            staging / "account_aliases.parquet",
        )
        for name, (schema, rows) in histories.items():
            write_table(
                connection,
                name,
                schema,
                rows,
                staging / f"{name}.parquet",
            )
    finally:
        connection.close()

    summary = {
        "schema_version": 1,
        "run_id": run_id,
        "built_at": isoformat(finished_at),
        "status": run_status,
        "lake_root": str(lake_root),
        "documents": len(inventory),
        "succeeded": status_counts["succeeded"],
        "partial": status_counts["partial"],
        "failed": status_counts["failed"],
        "unsupported": status_counts["unsupported"],
        "content_units": len(builder.content_units),
        "tables": len(builder.tables),
        "table_cells": len(builder.table_cells),
        "structured_values": len(builder.structured_values),
        "discovered_links": len(builder.links),
        "reported_line_candidates": len(builder.line_candidates),
        "reported_fact_candidates": len(builder.fact_candidates),
        "quality_events": len(builder.quality),
        "quality_errors": sum(row["severity"] == "error" for row in builder.quality),
        "quality_warnings": sum(
            row["severity"] == "warning" for row in builder.quality
        ),
        "ocr_pages": sum(row["ocr_page_count"] for row in builder.extractions),
        "mapping_version": matcher.mapping_version,
        "dataset_hashes": dataset_hashes,
        "manifest_path": manifest_relative.as_posix(),
        "snapshot_path": snapshot_relative.as_posix(),
        "published_to_current": published_to_current,
    }
    atomic_json_write(staging / "silver_build.json", summary)

    manifest_target = lake_root / manifest_relative
    manifest_target.parent.mkdir(parents=True, exist_ok=True)
    atomic_json_write(manifest_target, manifest)

    snapshot = lake_root / snapshot_relative
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    if snapshot.exists() or snapshot.is_symlink():
        raise ValueError(f"Silver snapshot already exists: {snapshot}")
    staging.replace(snapshot)
    if published_to_current:
        publish_current_snapshot(silver_root, snapshot)
    try:
        staging.parent.rmdir()
    except OSError:
        # A failed run may intentionally remain for diagnosis.
        pass
    return summary


def build_silver(
    lake_root: Path,
    account_aliases_path: Path,
    worker_path: Path,
    explicit_worker_python: Path | None,
) -> dict[str, Any]:
    """Serialize Silver publications so concurrent runs cannot lose history."""
    resolved_root = lake_root.resolve()
    silver_root = resolved_root / "silver"
    silver_root.mkdir(parents=True, exist_ok=True)
    lock_path = silver_root / "silver.lock"
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            return _build_silver_locked(
                resolved_root,
                account_aliases_path,
                worker_path,
                explicit_worker_python,
            )
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Raw PDF/XLSX/HTML/JSON into compact Silver Parquet tables."
    )
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument(
        "--account-aliases", type=Path, default=DEFAULT_ACCOUNT_ALIASES
    )
    parser.add_argument("--worker", type=Path, default=DEFAULT_WORKER)
    parser.add_argument("--worker-python", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = build_silver(
            args.lake_root,
            args.account_aliases,
            args.worker,
            args.worker_python,
        )
    except (OSError, ValueError, json.JSONDecodeError, duckdb.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return {"succeeded": 0, "partial": 2, "failed": 1}[summary["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
