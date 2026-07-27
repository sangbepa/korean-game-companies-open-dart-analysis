#!/usr/bin/env python3
"""Bootstrap the local game-accounting lakehouse without creating a database.

The command preserves the legacy ``data_lake`` tree, materializes its immutable
objects under ``game_accounting_lake/raw``, migrates collector state/manifests,
and rebuilds compact Parquet metadata indexes. DuckDB is used only as an
in-process Parquet writer and does not create a persistent catalog database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
import shutil
import sys
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlsplit

import duckdb

from collect_data_lake import atomic_json_write, load_sources


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_LEGACY_ROOT = PROJECT_ROOT / "data_lake"
DEFAULT_LAKE_ROOT = PROJECT_ROOT / "game_accounting_lake"
DEFAULT_COMPANIES = PROJECT_ROOT / "config" / "companies.json"
DEFAULT_SOURCES = PROJECT_ROOT / "config" / "sources.json"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
YEAR_PATTERN = re.compile(r"(?:^|[_/=-])((?:19|20)\d{2})(?:$|[_/?#-])")
SUCCESS_STATUSES = {"fetched", "unchanged", "not_modified"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Timestamp must be an ISO-8601 string, got {value!r}")
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_partition(path: Path, prefix: str) -> str | None:
    for part in path.parts:
        if part.startswith(prefix):
            return part.split("=", 1)[1]
    return None


def infer_year(*values: str | None) -> int | None:
    for value in values:
        if not value:
            continue
        match = YEAR_PATTERN.search(value)
        if match:
            return int(match.group(1))
    return None


def validate_iso_date(value: Any, field_name: str) -> str | None:
    if value in {None, ""}:
        return None
    rendered = str(value)
    try:
        date.fromisoformat(rendered)
    except ValueError as error:
        raise ValueError(f"Invalid {field_name}: {rendered!r}") from error
    return rendered


def normalize_alias(raw: Any) -> tuple[str, str, str | None, str | None, str | None]:
    if isinstance(raw, str):
        value = raw
        alias_type = "configured_alias"
        valid_from = None
        valid_to = None
        source_url = None
    elif isinstance(raw, dict):
        value = str(raw.get("value") or "")
        alias_type = str(raw.get("alias_type") or "configured_alias")
        valid_from = validate_iso_date(raw.get("valid_from"), "alias valid_from")
        valid_to = validate_iso_date(raw.get("valid_to"), "alias valid_to")
        source_url = str(raw["source_url"]) if raw.get("source_url") else None
    else:
        raise ValueError("Company aliases must be strings or objects")

    value = value.strip()
    if not value:
        raise ValueError("Company alias values must not be empty")
    if valid_from and valid_to and valid_from > valid_to:
        raise ValueError(f"Alias validity is reversed for {value!r}")
    return value, alias_type, valid_from, valid_to, source_url


def safe_relative_path(value: str, label: str, *, required_root: str | None = None) -> Path:
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Unsafe {label}: {value!r}")
    required_parts = Path(required_root).parts if required_root else ()
    if required_parts and relative.parts[: len(required_parts)] != required_parts:
        raise ValueError(f"{label} must be under {required_root}/: {value!r}")
    return relative


def confined_path(root: Path, relative: Path, label: str) -> Path:
    resolved_root = root.resolve()
    candidate = (resolved_root / relative).resolve(strict=False)
    if candidate != resolved_root and resolved_root not in candidate.parents:
        raise ValueError(f"{label} leaves configured root: {relative.as_posix()!r}")
    return candidate


def atomic_copy(source: Path, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    try:
        shutil.copy2(source, temporary)
        temporary.replace(destination)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_companies(path: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    companies = payload.get("companies")
    if not isinstance(companies, list) or not companies:
        raise ValueError("Company configuration must contain a non-empty companies list")

    company_ids: set[str] = set()
    aliases: dict[str, str] = {}
    identifier_keys: set[tuple[str, str, str]] = set()
    required = {
        "company_id",
        "legal_name",
        "display_name",
        "country_code",
        "reporting_currency",
        "fiscal_year_end_month",
        "official_website",
    }

    for company in companies:
        missing = sorted(required - set(company))
        if missing:
            raise ValueError(
                f"Company {company.get('company_id', '<unknown>')} is missing: "
                + ", ".join(missing)
            )
        company_id = str(company["company_id"])
        if not re.fullmatch(r"[a-z0-9][a-z0-9_]{1,63}", company_id):
            raise ValueError(f"Unsafe company_id: {company_id!r}")
        if company_id in company_ids:
            raise ValueError(f"Duplicate company_id: {company_id}")
        company_ids.add(company_id)

        month = int(company["fiscal_year_end_month"])
        if not 1 <= month <= 12:
            raise ValueError(f"Invalid fiscal_year_end_month for {company_id}")

        configured_aliases = [normalize_alias(value)[0] for value in company.get("aliases", [])]
        candidate_aliases = {
            company_id,
            str(company["display_name"]),
            str(company["legal_name"]),
            *configured_aliases,
        }
        for alias in candidate_aliases:
            key = alias.casefold().strip()
            previous = aliases.get(key)
            if previous and previous != company_id:
                raise ValueError(f"Company alias {alias!r} maps to multiple companies")
            aliases[key] = company_id

        for identifier in company.get("identifiers", []):
            scheme = str(identifier["scheme"])
            value = str(identifier["value"])
            market = str(identifier.get("market") or "")
            valid_from = validate_iso_date(
                identifier.get("valid_from"), "identifier valid_from"
            )
            valid_to = validate_iso_date(identifier.get("valid_to"), "identifier valid_to")
            if valid_from and valid_to and valid_from > valid_to:
                raise ValueError(f"Identifier validity is reversed for {scheme}:{value}")
            key = (scheme.casefold(), market.casefold(), value.casefold())
            if key in identifier_keys:
                raise ValueError(f"Duplicate company identifier: {key}")
            identifier_keys.add(key)

    return companies, aliases


def company_rows(companies: list[dict[str, Any]]) -> tuple[list[tuple[Any, ...]], ...]:
    masters: list[tuple[Any, ...]] = []
    aliases: list[tuple[Any, ...]] = []
    identifiers: list[tuple[Any, ...]] = []
    for company in companies:
        company_id = str(company["company_id"])
        masters.append(
            (
                company_id,
                str(company["legal_name"]),
                str(company["display_name"]),
                validate_iso_date(
                    company.get("legal_name_valid_from"), "legal_name_valid_from"
                ),
                str(company["country_code"]),
                str(company["reporting_currency"]),
                int(company["fiscal_year_end_month"]),
                str(company["official_website"]),
                bool(company.get("active", True)),
            )
        )
        all_aliases = [
            (company_id, "company_id", None, None, None),
            (str(company["display_name"]), "display_name", None, None, None),
            (str(company["legal_name"]), "legal_name", None, None, None),
            *(normalize_alias(value) for value in company.get("aliases", [])),
        ]
        seen: set[tuple[str, str, str | None, str | None]] = set()
        for alias, alias_type, valid_from, valid_to, source_url in all_aliases:
            key = (alias.casefold().strip(), alias_type, valid_from, valid_to)
            if key in seen:
                continue
            seen.add(key)
            aliases.append(
                (company_id, alias, alias_type, valid_from, valid_to, source_url)
            )

        for identifier in company.get("identifiers", []):
            identifiers.append(
                (
                    company_id,
                    str(identifier["scheme"]),
                    str(identifier["value"]),
                    identifier.get("market"),
                    identifier.get("valid_from"),
                    identifier.get("valid_to"),
                    bool(identifier.get("is_primary", False)),
                    identifier.get("source_url"),
                    str(identifier.get("verification_status", "verified")),
                )
            )
    return masters, aliases, identifiers


def source_registry_rows(
    source_config: Path,
    aliases: dict[str, str],
    historical_sources: dict[str, dict[str, Any]],
) -> tuple[list[tuple[Any, ...]], dict[str, str]]:
    sources = load_sources(source_config)
    rows: list[tuple[Any, ...]] = []
    source_company: dict[str, str] = {}

    for source in sources:
        company_id = aliases.get(source.company.casefold().strip())
        if not company_id:
            raise ValueError(f"Source company is not mapped: {source.company!r}")
        source_company[source.id] = company_id
        rows.append(
            (
                source.id,
                company_id,
                source.category,
                source.method,
                source.url,
                urlsplit(source.url).hostname,
                bool(source.discover_documents),
                int(source.max_documents),
                json.dumps(source.allowed_hosts, ensure_ascii=False),
                json.dumps([name for name, _ in source.request_headers]),
                json.dumps(source.link_allow_patterns, ensure_ascii=False),
                json.dumps(source.json_id_fields, ensure_ascii=False),
                source.json_url_template or None,
                source.notes,
                True,
            )
        )

    company_ids = sorted(set(aliases.values()), key=len, reverse=True)
    for source_id, sample in sorted(historical_sources.items()):
        if source_id in source_company:
            continue
        company_name = str(sample.get("company") or "").casefold().strip()
        company_id = aliases.get(company_name)
        if not company_id:
            company_id = next(
                (
                    candidate
                    for candidate in company_ids
                    if source_id == candidate or source_id.startswith(candidate + "_")
                ),
                None,
            )
        if company_id:
            source_company[source_id] = company_id
        url = sample.get("url")
        rows.append(
            (
                source_id,
                company_id,
                sample.get("category"),
                sample.get("request_method"),
                url,
                urlsplit(url).hostname if url else None,
                False,
                0,
                "[]",
                "[]",
                "[]",
                "[]",
                None,
                "Historical source discovered in a legacy manifest.",
                False,
            )
        )
    return rows, source_company


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "objects_by_sha256": {}, "urls": {}}
    with path.open(encoding="utf-8") as handle:
        state = json.load(handle)
    if state.get("version") != 1:
        raise ValueError(f"Unsupported collector state version in {path}")
    state.setdefault("objects_by_sha256", {})
    state.setdefault("urls", {})
    if not isinstance(state["objects_by_sha256"], dict) or not isinstance(
        state["urls"], dict
    ):
        raise ValueError(f"Collector state indexes must be mappings in {path}")
    return state


def target_raw_path(sha256: str, legacy_path: Path) -> Path:
    original_name = legacy_path.name.split("__", 1)[-1]
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", original_name)[:160] or "object.bin"
    return Path("raw") / "objects" / f"sha256={sha256[:2]}" / f"{sha256}__{safe_name}"


def same_inode(first: Path, second: Path) -> bool:
    first_stat = first.stat()
    second_stat = second.stat()
    return first_stat.st_dev == second_stat.st_dev and first_stat.st_ino == second_stat.st_ino


def materialize_legacy_objects(
    legacy_root: Path,
    lake_root: Path,
    legacy_state: dict[str, Any],
    target_state: dict[str, Any],
    strategy: str,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    object_info: dict[str, dict[str, Any]] = {}
    quality: list[dict[str, str]] = []

    for sha256, relative_string in sorted(legacy_state["objects_by_sha256"].items()):
        if not SHA256_PATTERN.fullmatch(sha256):
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "legacy_object",
                    "entity_id": sha256,
                    "check_name": "valid_sha256",
                    "message": f"Invalid SHA-256 key for {relative_string}",
                }
            )
            continue
        legacy_relative = safe_relative_path(relative_string, "legacy object path")
        legacy_path = confined_path(legacy_root, legacy_relative, "legacy object path")
        source_id = extract_partition(legacy_relative, "source=") or "unknown_source"
        existing_relative_string = target_state["objects_by_sha256"].get(sha256)
        if existing_relative_string:
            target_relative = safe_relative_path(
                str(existing_relative_string),
                "target object path",
                required_root="raw/objects",
            )
        else:
            target_relative = target_raw_path(sha256, legacy_relative)
        target_path = confined_path(lake_root, target_relative, "target object path")

        if not legacy_path.is_file():
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "document",
                    "entity_id": sha256,
                    "check_name": "legacy_file_exists",
                    "message": f"Missing legacy file: {legacy_path}",
                }
            )
            continue
        if sha256_file(legacy_path) != sha256:
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "document",
                    "entity_id": sha256,
                    "check_name": "legacy_checksum",
                    "message": f"Legacy checksum mismatch: {legacy_path}",
                }
            )
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        if not target_path.exists():
            if strategy == "hardlink":
                try:
                    os.link(legacy_path, target_path)
                except OSError:
                    atomic_copy(legacy_path, target_path)
            else:
                atomic_copy(legacy_path, target_path)
        if sha256_file(target_path) != sha256:
            raise ValueError(f"Refusing conflicting Raw object: {target_path}")

        mode = "hardlink" if same_inode(legacy_path, target_path) else "copy"
        target_state["objects_by_sha256"][sha256] = target_relative.as_posix()
        object_info[sha256] = {
            "source_id": source_id,
            "legacy_path": legacy_relative.as_posix(),
            "raw_path": target_relative.as_posix(),
            "materialization_mode": mode,
        }

    # Preserve objects collected directly into the new lakehouse.
    for sha256, relative_string in sorted(target_state["objects_by_sha256"].items()):
        if sha256 in object_info:
            continue
        if not SHA256_PATTERN.fullmatch(str(sha256)):
            raise ValueError(f"Invalid checksum in target collector state: {sha256!r}")
        target_relative = safe_relative_path(
            str(relative_string), "target object path", required_root="raw/objects"
        )
        target_path = confined_path(lake_root, target_relative, "target object path")
        if target_path.is_file() and sha256_file(target_path) == sha256:
            object_info[sha256] = {
                "source_id": extract_partition(target_relative, "source="),
                "legacy_path": None,
                "raw_path": target_relative.as_posix(),
                "materialization_mode": "native",
            }
        else:
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "document",
                    "entity_id": sha256,
                    "check_name": "native_raw_checksum",
                    "message": f"Missing or invalid native raw object: {target_path}",
                }
            )

    # Merge conditional-request cache and rewrite legacy object paths.
    for cache_key, cached in legacy_state["urls"].items():
        if not isinstance(cached, dict):
            raise ValueError(f"Invalid legacy URL cache entry: {cache_key!r}")
        migrated = dict(cached)
        sha256 = migrated.get("checksum_sha256")
        if sha256 in object_info:
            migrated["object_path"] = object_info[sha256]["raw_path"]
        elif migrated.get("object_path"):
            migrated["object_path"] = None
        existing = target_state["urls"].get(cache_key)
        if not existing or str(migrated.get("last_seen_at") or "") >= str(
            existing.get("last_seen_at") or ""
        ):
            target_state["urls"][cache_key] = migrated

    for cache_key, cached in target_state["urls"].items():
        if not isinstance(cached, dict):
            raise ValueError(f"Invalid target URL cache entry: {cache_key!r}")
        relative_string = cached.get("object_path")
        if relative_string:
            safe_relative_path(
                str(relative_string),
                "target URL cache object path",
                required_root="raw/objects",
            )

    return object_info, quality


def copy_legacy_manifests(legacy_root: Path, lake_root: Path) -> int:
    source_root = legacy_root / "manifests"
    destination_root = lake_root / "metadata" / "manifests_json"
    copied = 0
    if not source_root.exists():
        return copied
    for source in sorted(source_root.rglob("*.json")):
        relative = source.relative_to(source_root)
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if sha256_file(destination) == sha256_file(source):
                continue
            destination = destination.with_name(
                f"{destination.stem}_legacy_{sha256_file(source)[:8]}.json"
            )
            if destination.exists():
                if sha256_file(destination) == sha256_file(source):
                    continue
                raise ValueError(f"Conflicting legacy manifest destination: {destination}")
        shutil.copy2(source, destination)
        copied += 1
    return copied


def load_manifest_events(
    lake_root: Path,
    object_info: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    manifest_root = lake_root / "metadata" / "manifests_json"
    events: list[dict[str, Any]] = []
    runs: list[dict[str, Any]] = []
    historical_sources: dict[str, dict[str, Any]] = {}
    seen_manifest_hashes: set[str] = set()
    run_hashes: dict[str, str] = {}
    for manifest_path in sorted(manifest_root.rglob("*.json")):
        with manifest_path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
        if not isinstance(manifest, dict):
            raise ValueError(f"Manifest must be an object: {manifest_path}")
        manifest_sha256 = sha256_file(manifest_path)
        if manifest_sha256 in seen_manifest_hashes:
            continue
        seen_manifest_hashes.add(manifest_sha256)
        run_id = str(manifest.get("run_id") or manifest_path.stem)
        previous_hash = run_hashes.get(run_id)
        if previous_hash and previous_hash != manifest_sha256:
            raise ValueError(f"Conflicting manifests share run_id {run_id!r}")
        run_hashes[run_id] = manifest_sha256
        runs.append(
            {
                "run_id": run_id,
                "manifest_version": manifest.get("manifest_version"),
                "started_at": parse_timestamp(manifest.get("started_at")),
                "finished_at": parse_timestamp(manifest.get("finished_at")),
                "source_count": manifest.get("source_count"),
                "request_count": manifest.get("request_count"),
                "status_counts_json": json.dumps(
                    manifest.get("status_counts") or {}, sort_keys=True
                ),
                "manifest_path": manifest_path.relative_to(lake_root).as_posix(),
                "manifest_sha256": manifest_sha256,
            }
        )
        results = manifest.get("results", [])
        if not isinstance(results, list):
            raise ValueError(f"Manifest results must be a list: {manifest_path}")
        for index, result in enumerate(results):
            if not isinstance(result, dict):
                raise ValueError(
                    f"Manifest result {index} must be an object: {manifest_path}"
                )
            source_id = str(result.get("source_id") or "unknown_source")
            historical_sample = historical_sources.setdefault(source_id, {})
            for key, value in result.items():
                previous_value = historical_sample.get(key)
                if (
                    value is not None
                    and value != ""
                    and (previous_value is None or previous_value == "")
                ):
                    historical_sample[key] = value
            sha256 = result.get("checksum_sha256")
            raw_path = object_info.get(sha256, {}).get("raw_path")
            event_key = f"{run_id}:{index}:{source_id}:{result.get('url')}"
            events.append(
                {
                    "event_id": hashlib.sha256(event_key.encode()).hexdigest(),
                    "run_id": run_id,
                    "run_started_at": parse_timestamp(manifest.get("started_at")),
                    "run_finished_at": parse_timestamp(manifest.get("finished_at")),
                    "source_id": source_id,
                    "category": result.get("category"),
                    "request_method": result.get("request_method"),
                    "request_url": result.get("url"),
                    "parent_url": result.get("parent_url"),
                    "final_url": result.get("final_url"),
                    "retrieved_at": parse_timestamp(result.get("retrieved_at")),
                    "status": result.get("status"),
                    "http_status": result.get("http_status"),
                    "content_type": result.get("content_type"),
                    "size_bytes": result.get("size_bytes"),
                    "sha256": sha256,
                    "document_id": sha256 if sha256 in object_info else None,
                    "raw_path": raw_path,
                    "report_year": infer_year(source_id, result.get("url")),
                    "etag": result.get("etag"),
                    "last_modified": result.get("last_modified"),
                    "error": result.get("error"),
                    "manifest_path": manifest_path.relative_to(lake_root).as_posix(),
                }
            )
    return events, runs, historical_sources


def document_rows(
    lake_root: Path,
    object_info: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    quality: list[dict[str, str]],
) -> list[tuple[Any, ...]]:
    event_by_sha: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event.get("sha256"):
            event_by_sha[event["sha256"]].append(event)

    rows: list[tuple[Any, ...]] = []
    for sha256, info in sorted(object_info.items()):
        raw_path = lake_root / info["raw_path"]
        suffix = raw_path.suffix.casefold()
        linked_events = event_by_sha.get(sha256, [])
        content_type = next(
            (event.get("content_type") for event in linked_events if event.get("content_type")),
            None,
        ) or mimetypes.guess_type(raw_path.name)[0]
        timestamps = [
            event["retrieved_at"]
            for event in linked_events
            if event.get("retrieved_at") is not None
        ]
        first_seen = min(timestamps) if timestamps else None
        last_seen = max(timestamps) if timestamps else None
        kind = "source_snapshot" if suffix in {".html", ".json", ".xml"} else "source_document"

        with raw_path.open("rb") as handle:
            signature = handle.read(5)
        integrity_status = "valid"
        if suffix == ".pdf" and not signature.startswith(b"%PDF-"):
            integrity_status = "invalid_signature"
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "document",
                    "entity_id": sha256,
                    "check_name": "pdf_signature",
                    "message": f"PDF signature missing: {info['raw_path']}",
                }
            )
        if suffix == ".xlsx" and not signature.startswith(b"PK"):
            integrity_status = "invalid_signature"
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "document",
                    "entity_id": sha256,
                    "check_name": "xlsx_signature",
                    "message": f"XLSX ZIP signature missing: {info['raw_path']}",
                }
            )

        rows.append(
            (
                sha256,
                sha256,
                raw_path.stat().st_size,
                suffix or None,
                content_type,
                kind,
                info["raw_path"],
                info.get("legacy_path"),
                info.get("source_id"),
                info["materialization_mode"],
                first_seen,
                last_seen,
                integrity_status,
            )
        )
    return rows


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def write_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    schema: Sequence[tuple[str, str]],
    rows: Sequence[Sequence[Any]],
    destination: Path,
) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    definitions = ", ".join(f'"{name}" {kind}' for name, kind in schema)
    connection.execute(f"CREATE TABLE {table_name} ({definitions})")
    if rows:
        placeholders = ", ".join("?" for _ in schema)
        connection.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(
        f".{destination.stem}.{uuid.uuid4().hex}.tmp.parquet"
    )
    connection.execute(
        f"COPY {table_name} TO {sql_literal(str(temporary))} "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    temporary.replace(destination)


def build_parquet_metadata(
    lake_root: Path,
    companies: list[dict[str, Any]],
    aliases: dict[str, str],
    source_config: Path,
    object_info: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    historical_sources: dict[str, dict[str, Any]],
    quality: list[dict[str, str]],
    checked_at: datetime,
) -> dict[str, int]:
    masters, alias_rows, identifier_rows = company_rows(companies)
    registry_rows, source_company = source_registry_rows(
        source_config, aliases, historical_sources
    )
    document_data = document_rows(lake_root, object_info, events, quality)
    known_documents = set(object_info)

    event_rows: list[tuple[Any, ...]] = []
    for event in events:
        company_id = source_company.get(event["source_id"])
        if not company_id:
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "ingestion_event",
                    "entity_id": event["event_id"],
                    "check_name": "source_company_resolved",
                    "message": f"No company mapping for source {event['source_id']!r}",
                }
            )
        sha256 = event.get("sha256")
        if sha256 and not SHA256_PATTERN.fullmatch(str(sha256)):
            quality.append(
                {
                    "severity": "error",
                    "entity_type": "ingestion_event",
                    "entity_id": event["event_id"],
                    "check_name": "valid_sha256",
                    "message": f"Invalid event checksum: {sha256!r}",
                }
            )
        if event.get("status") in SUCCESS_STATUSES:
            if not sha256:
                quality.append(
                    {
                        "severity": "error",
                        "entity_type": "ingestion_event",
                        "entity_id": event["event_id"],
                        "check_name": "successful_event_has_checksum",
                        "message": "Successful retrieval has no checksum.",
                    }
                )
            elif sha256 not in known_documents:
                quality.append(
                    {
                        "severity": "error",
                        "entity_type": "ingestion_event",
                        "entity_id": event["event_id"],
                        "check_name": "successful_event_document_exists",
                        "message": f"Successful retrieval references missing Raw object {sha256}.",
                    }
                )
        event_rows.append(
            (
                event["event_id"],
                event["run_id"],
                event["run_started_at"],
                event["run_finished_at"],
                event["source_id"],
                company_id,
                event["category"],
                event["request_method"],
                event["request_url"],
                event["parent_url"],
                event["final_url"],
                event["retrieved_at"],
                event["status"],
                event["http_status"],
                event["content_type"],
                event["size_bytes"],
                event["sha256"],
                event["document_id"],
                event["raw_path"],
                event["report_year"],
                event["etag"],
                event["last_modified"],
                event["error"],
                event["manifest_path"],
            )
        )

    run_rows = [
        (
            run["run_id"],
            run["manifest_version"],
            run["started_at"],
            run["finished_at"],
            run["source_count"],
            run["request_count"],
            run["status_counts_json"],
            run["manifest_path"],
            run["manifest_sha256"],
        )
        for run in runs
    ]

    quality_rows = [
        (
            hashlib.sha256(
                f"{item['entity_type']}:{item['entity_id']}:{item['check_name']}".encode()
            ).hexdigest(),
            checked_at,
            item["severity"],
            item["entity_type"],
            item["entity_id"],
            item["check_name"],
            item["message"],
        )
        for item in quality
    ]

    metadata = lake_root / "metadata"
    connection = duckdb.connect(":memory:")
    try:
        write_table(
            connection,
            "company_master",
            (
                ("company_id", "VARCHAR"),
                ("legal_name", "VARCHAR"),
                ("display_name", "VARCHAR"),
                ("legal_name_valid_from", "DATE"),
                ("country_code", "VARCHAR"),
                ("reporting_currency", "VARCHAR"),
                ("fiscal_year_end_month", "UTINYINT"),
                ("official_website", "VARCHAR"),
                ("active", "BOOLEAN"),
            ),
            masters,
            metadata / "company_master.parquet",
        )
        write_table(
            connection,
            "company_aliases",
            (
                ("company_id", "VARCHAR"),
                ("alias", "VARCHAR"),
                ("alias_type", "VARCHAR"),
                ("valid_from", "DATE"),
                ("valid_to", "DATE"),
                ("source_url", "VARCHAR"),
            ),
            alias_rows,
            metadata / "company_aliases.parquet",
        )
        write_table(
            connection,
            "company_identifiers",
            (
                ("company_id", "VARCHAR"),
                ("identifier_scheme", "VARCHAR"),
                ("identifier_value", "VARCHAR"),
                ("market", "VARCHAR"),
                ("valid_from", "DATE"),
                ("valid_to", "DATE"),
                ("is_primary", "BOOLEAN"),
                ("source_url", "VARCHAR"),
                ("verification_status", "VARCHAR"),
            ),
            identifier_rows,
            metadata / "company_identifiers.parquet",
        )
        write_table(
            connection,
            "source_registry",
            (
                ("source_id", "VARCHAR"),
                ("company_id", "VARCHAR"),
                ("category", "VARCHAR"),
                ("request_method", "VARCHAR"),
                ("url", "VARCHAR"),
                ("seed_host", "VARCHAR"),
                ("discover_documents", "BOOLEAN"),
                ("max_documents", "USMALLINT"),
                ("allowed_hosts_json", "JSON"),
                ("request_header_names_json", "JSON"),
                ("link_allow_patterns_json", "JSON"),
                ("json_id_fields_json", "JSON"),
                ("json_url_template", "VARCHAR"),
                ("notes", "VARCHAR"),
                ("active", "BOOLEAN"),
            ),
            registry_rows,
            metadata / "source_registry.parquet",
        )
        write_table(
            connection,
            "documents",
            (
                ("document_id", "VARCHAR"),
                ("sha256", "VARCHAR"),
                ("size_bytes", "BIGINT"),
                ("file_extension", "VARCHAR"),
                ("content_type", "VARCHAR"),
                ("document_kind", "VARCHAR"),
                ("raw_path", "VARCHAR"),
                ("legacy_path", "VARCHAR"),
                ("legacy_storage_source_id", "VARCHAR"),
                ("materialization_mode", "VARCHAR"),
                ("first_seen_at", "TIMESTAMPTZ"),
                ("last_seen_at", "TIMESTAMPTZ"),
                ("integrity_status", "VARCHAR"),
            ),
            document_data,
            metadata / "documents.parquet",
        )
        write_table(
            connection,
            "ingestion_runs",
            (
                ("run_id", "VARCHAR"),
                ("manifest_version", "INTEGER"),
                ("started_at", "TIMESTAMPTZ"),
                ("finished_at", "TIMESTAMPTZ"),
                ("source_count", "INTEGER"),
                ("request_count", "INTEGER"),
                ("status_counts_json", "JSON"),
                ("manifest_path", "VARCHAR"),
                ("manifest_sha256", "VARCHAR"),
            ),
            run_rows,
            metadata / "ingestion_runs.parquet",
        )
        write_table(
            connection,
            "ingestion_manifest",
            (
                ("event_id", "VARCHAR"),
                ("run_id", "VARCHAR"),
                ("run_started_at", "TIMESTAMPTZ"),
                ("run_finished_at", "TIMESTAMPTZ"),
                ("source_id", "VARCHAR"),
                ("company_id", "VARCHAR"),
                ("category", "VARCHAR"),
                ("request_method", "VARCHAR"),
                ("request_url", "VARCHAR"),
                ("parent_url", "VARCHAR"),
                ("final_url", "VARCHAR"),
                ("retrieved_at", "TIMESTAMPTZ"),
                ("status", "VARCHAR"),
                ("http_status", "INTEGER"),
                ("content_type", "VARCHAR"),
                ("size_bytes", "BIGINT"),
                ("sha256", "VARCHAR"),
                ("document_id", "VARCHAR"),
                ("raw_path", "VARCHAR"),
                ("report_year", "INTEGER"),
                ("etag", "VARCHAR"),
                ("last_modified", "VARCHAR"),
                ("error", "VARCHAR"),
                ("manifest_path", "VARCHAR"),
            ),
            event_rows,
            metadata / "ingestion_manifest.parquet",
        )

        connection.execute(
            """
            CREATE OR REPLACE TABLE source_documents AS
            SELECT
                document_id,
                source_id,
                min(company_id) AS company_id,
                request_url AS source_url,
                report_year,
                min(retrieved_at) AS first_retrieved_at,
                max(retrieved_at) AS last_retrieved_at,
                count(*)::BIGINT AS observation_count,
                min(raw_path) AS raw_path
            FROM ingestion_manifest
            WHERE document_id IS NOT NULL
              AND status IN ('fetched', 'unchanged', 'not_modified')
            GROUP BY document_id, source_id, request_url, report_year
            """
        )
        source_document_count = connection.execute(
            "SELECT count(*) FROM source_documents"
        ).fetchone()[0]
        temporary = metadata / f".source_documents.{uuid.uuid4().hex}.tmp.parquet"
        connection.execute(
            f"COPY source_documents TO {sql_literal(str(temporary))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        temporary.replace(metadata / "source_documents.parquet")

        write_table(
            connection,
            "data_quality_log",
            (
                ("quality_event_id", "VARCHAR"),
                ("checked_at", "TIMESTAMPTZ"),
                ("severity", "VARCHAR"),
                ("entity_type", "VARCHAR"),
                ("entity_id", "VARCHAR"),
                ("check_name", "VARCHAR"),
                ("message", "VARCHAR"),
            ),
            quality_rows,
            metadata / "data_quality_log.parquet",
        )
    finally:
        connection.close()

    return {
        "companies": len(masters),
        "company_aliases": len(alias_rows),
        "company_identifiers": len(identifier_rows),
        "sources": len(registry_rows),
        "documents": len(document_data),
        "ingestion_runs": len(run_rows),
        "ingestion_events": len(event_rows),
        "source_documents": int(source_document_count),
        "quality_issues": len(quality_rows),
    }


def ensure_layout(lake_root: Path) -> None:
    for relative in (
        "raw",
        "silver",
        "gold",
        "metadata",
        "metadata/manifests_json",
        "catalog",
    ):
        (lake_root / relative).mkdir(parents=True, exist_ok=True)


def build_lakehouse(
    legacy_root: Path,
    lake_root: Path,
    companies_path: Path,
    source_config: Path,
    strategy: str,
) -> dict[str, Any]:
    legacy_root = legacy_root.resolve()
    lake_root = lake_root.resolve()
    if strategy not in {"copy", "hardlink"}:
        raise ValueError(f"Unknown materialization strategy: {strategy!r}")
    if (
        legacy_root == lake_root
        or legacy_root in lake_root.parents
        or lake_root in legacy_root.parents
    ):
        raise ValueError("Legacy and target lake roots must not overlap")
    ensure_layout(lake_root)
    companies, aliases = load_companies(companies_path)

    legacy_state = load_state(legacy_root / "state" / "collector_state.json")
    target_state_path = lake_root / "metadata" / "collector_state.json"
    target_state = load_state(target_state_path)
    object_info, quality = materialize_legacy_objects(
        legacy_root, lake_root, legacy_state, target_state, strategy
    )
    atomic_json_write(target_state_path, target_state)
    copied_manifests = copy_legacy_manifests(legacy_root, lake_root)
    events, runs, historical_sources = load_manifest_events(lake_root, object_info)
    checked_at = utc_now()
    counts = build_parquet_metadata(
        lake_root,
        companies,
        aliases,
        source_config,
        object_info,
        events,
        runs,
        historical_sources,
        quality,
        checked_at,
    )
    summary = {
        "schema_version": 1,
        "built_at": isoformat(checked_at),
        "legacy_root": str(legacy_root),
        "lake_root": str(lake_root),
        "materialization_strategy": strategy,
        "copied_legacy_manifests": copied_manifests,
        **counts,
    }
    atomic_json_write(lake_root / "metadata" / "metadata_build.json", summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap Raw storage and Parquet metadata for the local lakehouse."
    )
    parser.add_argument("--legacy-root", type=Path, default=DEFAULT_LEGACY_ROOT)
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument("--companies", type=Path, default=DEFAULT_COMPANIES)
    parser.add_argument("--sources", type=Path, default=DEFAULT_SOURCES)
    parser.add_argument(
        "--materialization",
        choices=("hardlink", "copy"),
        default="copy",
        help="Hardlink immutable legacy objects when possible; otherwise copy.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = build_lakehouse(
            args.legacy_root,
            args.lake_root,
            args.companies,
            args.sources,
            args.materialization,
        )
    except (OSError, ValueError, json.JSONDecodeError, duckdb.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
