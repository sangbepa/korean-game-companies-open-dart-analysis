#!/usr/bin/env python3
"""Collect immutable snapshots from configured public data sources.

The collector intentionally performs only shallow collection: it saves each
configured seed page and optionally downloads document links found directly on
that page. Parsing and financial normalization belong to a later pipeline.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import html as html_module
import json
import mimetypes
import os
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.message import Message
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


DEFAULT_CONFIG = Path(__file__).resolve().parent / "config" / "sources.json"
DEFAULT_LAKE_ROOT = Path(__file__).resolve().parent / "game_accounting_lake"
DEFAULT_USER_AGENT = "mobile-game-data-lake/0.1 (public IR research collector)"
DEFAULT_DOCUMENT_PATTERNS = (
    r"(?i)\.(?:pdf|xlsx?|csv|tsv|json|xml|zip)(?:$|[?#])",
    r"(?i)/download(?:/|[?])",
)
CONTENT_TYPE_EXTENSIONS = {
    "application/pdf": ".pdf",
    "application/json": ".json",
    "application/xml": ".xml",
    "application/zip": ".zip",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "text/csv": ".csv",
    "text/html": ".html",
    "text/plain": ".txt",
    "text/tab-separated-values": ".tsv",
}
SAFE_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_-]{1,79}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    """Render UTC timestamps consistently."""
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_json_write(path: Path, payload: Any) -> None:
    """Write JSON atomically so interrupted runs cannot corrupt state."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def validated_object_path(lake_root: Path, relative_string: str) -> Path:
    """Resolve a state object path without allowing it to leave Raw storage."""
    relative = Path(relative_string)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Unsafe object path in collector state: {relative_string!r}")
    if len(relative.parts) < 2 or relative.parts[:2] != ("raw", "objects"):
        raise ValueError(
            f"Collector object path must be under raw/objects/: {relative_string!r}"
        )
    root = lake_root.resolve()
    candidate = (root / relative).resolve(strict=False)
    if candidate != root and root not in candidate.parents:
        raise ValueError(f"Object path leaves lake root: {relative_string!r}")
    return candidate


@dataclass(frozen=True)
class Source:
    """Validated source configuration."""

    id: str
    company: str
    category: str
    url: str
    allowed_hosts: tuple[str, ...]
    method: str = "GET"
    request_body: str = ""
    request_headers: tuple[tuple[str, str], ...] = ()
    discover_documents: bool = True
    max_documents: int = 20
    link_allow_patterns: tuple[str, ...] = DEFAULT_DOCUMENT_PATTERNS
    json_id_fields: tuple[str, ...] = ()
    json_url_template: str = ""
    notes: str = ""

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "Source":
        required = ("id", "company", "category", "url", "allowed_hosts")
        missing = [name for name in required if not raw.get(name)]
        if missing:
            raise ValueError(f"Source is missing required fields: {', '.join(missing)}")

        source_id = str(raw["id"])
        category = str(raw["category"])
        if not SAFE_IDENTIFIER.fullmatch(source_id):
            raise ValueError(f"Unsafe source id: {source_id!r}")
        if not SAFE_IDENTIFIER.fullmatch(category):
            raise ValueError(f"Unsafe source category: {category!r}")

        url = str(raw["url"])
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"Source URL must be HTTP(S): {url!r}")

        allowed_hosts = tuple(str(host).casefold() for host in raw["allowed_hosts"])
        if parsed.hostname.casefold() not in allowed_hosts:
            raise ValueError(
                f"Seed host {parsed.hostname!r} is absent from allowed_hosts for {source_id}"
            )

        patterns = tuple(raw.get("link_allow_patterns") or DEFAULT_DOCUMENT_PATTERNS)
        for pattern in patterns:
            re.compile(pattern)

        max_documents = int(raw.get("max_documents", 20))
        if not 0 <= max_documents <= 200:
            raise ValueError(f"max_documents must be between 0 and 200 for {source_id}")

        method = str(raw.get("method", "GET")).upper()
        if method not in {"GET", "POST"}:
            raise ValueError(f"Only GET and POST sources are supported for {source_id}")

        raw_headers = raw.get("request_headers", {})
        if not isinstance(raw_headers, dict):
            raise ValueError(f"request_headers must be an object for {source_id}")
        forbidden_headers = {"authorization", "host", "content-length"}
        requested_forbidden = forbidden_headers & {
            str(name).casefold() for name in raw_headers
        }
        if requested_forbidden:
            raise ValueError(
                f"Forbidden configured headers for {source_id}: "
                + ", ".join(sorted(requested_forbidden))
            )
        request_headers = tuple(
            (str(name), str(value)) for name, value in raw_headers.items()
        )

        json_id_fields = tuple(str(name) for name in raw.get("json_id_fields", []))
        json_url_template = str(raw.get("json_url_template", ""))
        if bool(json_id_fields) != bool(json_url_template):
            raise ValueError(
                f"json_id_fields and json_url_template must be set together for {source_id}"
            )
        if json_url_template:
            if "{value}" not in json_url_template:
                raise ValueError(
                    f"json_url_template must contain {{value}} for {source_id}"
                )
            template_host = urlsplit(json_url_template.format(value="1")).hostname
            if not template_host or template_host.casefold() not in allowed_hosts:
                raise ValueError(
                    f"json_url_template host is absent from allowed_hosts for {source_id}"
                )

        return cls(
            id=source_id,
            company=str(raw["company"]),
            category=category,
            url=url,
            allowed_hosts=allowed_hosts,
            method=method,
            request_body=str(raw.get("request_body", "")),
            request_headers=request_headers,
            discover_documents=bool(raw.get("discover_documents", True)),
            max_documents=max_documents,
            link_allow_patterns=patterns,
            json_id_fields=json_id_fields,
            json_url_template=json_url_template,
            notes=str(raw.get("notes", "")),
        )


@dataclass
class FetchResult:
    """One URL retrieval event written to a run manifest."""

    source_id: str
    company: str
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
    etag: str | None = None
    last_modified: str | None = None
    error: str | None = None
    data: bytes | None = field(default=None, repr=False)

    def manifest_mapping(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("data", None)
        return payload


class LinkParser(HTMLParser):
    """Collect anchors without importing a browser or HTML dependency."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.casefold() != "a":
            return
        for name, value in attrs:
            if name.casefold() == "href" and value:
                self.links.append(value.strip())


class RestrictedRedirectHandler(HTTPRedirectHandler):
    """Reject redirects that leave a source's explicit host allowlist."""

    def __init__(self, allowed_hosts: Iterable[str]) -> None:
        super().__init__()
        self.allowed_hosts = {host.casefold() for host in allowed_hosts}

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> Request | None:
        parsed = urlsplit(newurl)
        if parsed.scheme not in {"http", "https"}:
            raise HTTPError(newurl, code, "Blocked non-HTTP redirect", headers, fp)
        if not parsed.hostname or parsed.hostname.casefold() not in self.allowed_hosts:
            raise HTTPError(newurl, code, "Blocked redirect outside allowed_hosts", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class RateLimiter:
    """Apply a polite minimum interval independently to each host."""

    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(0.0, interval_seconds)
        self.last_request: dict[str, float] = {}

    def wait(self, url: str) -> None:
        host = (urlsplit(url).hostname or "").casefold()
        previous = self.last_request.get(host)
        if previous is not None:
            remaining = self.interval_seconds - (time.monotonic() - previous)
            if remaining > 0:
                time.sleep(remaining)
        self.last_request[host] = time.monotonic()


def load_sources(config_path: Path) -> list[Source]:
    """Load and validate source definitions."""
    with config_path.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    items = raw.get("sources")
    if not isinstance(items, list):
        raise ValueError("Configuration must contain a 'sources' list")
    sources = [Source.from_mapping(item) for item in items]
    ids = [source.id for source in sources]
    if len(ids) != len(set(ids)):
        raise ValueError("Source ids must be unique")
    return sources


def normalized_url(url: str) -> str:
    """Remove fragments because they do not change downloaded content."""
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def discover_document_urls(html: bytes, base_url: str, source: Source) -> list[str]:
    """Return shallow, allowlisted document URLs from HTML or JSON text."""
    parser = LinkParser()
    decoded = html.decode("utf-8", errors="replace")
    parser.feed(decoded)
    patterns = [re.compile(pattern) for pattern in source.link_allow_patterns]
    results: list[str] = []
    seen: set[str] = set()

    # Modern IR sites often embed download URLs inside JSON application state
    # rather than real anchor tags. Include those absolute URLs as candidates.
    script_text = decoded.replace(r"\/", "/").replace(r"\u0026", "&")
    absolute_urls = re.findall(r"https?://[^\s\"'<>\\]+", script_text)

    templated_urls: list[str] = []
    if source.json_id_fields and source.json_url_template:
        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            payload = None

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in source.json_id_fields and child not in {None, "", 0, "0"}:
                        templated_urls.append(
                            source.json_url_template.format(value=str(child))
                        )
                    else:
                        visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        visit(payload)

    for href in [*parser.links, *absolute_urls, *templated_urls]:
        href = html_module.unescape(href).rstrip(").,;]")
        candidate = normalized_url(urljoin(base_url, href))
        parsed = urlsplit(candidate)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            continue
        if parsed.hostname.casefold() not in source.allowed_hosts:
            continue
        if not any(pattern.search(candidate) for pattern in patterns):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        results.append(candidate)
        if len(results) >= source.max_documents:
            break
    return results


def media_type(value: str | None) -> str:
    """Return the lowercase MIME type without parameters."""
    return (value or "application/octet-stream").split(";", 1)[0].strip().casefold()


def safe_basename(url: str, headers: Message, content_type: str) -> str:
    """Build a readable, filesystem-safe name for a fetched object."""
    disposition = headers.get("Content-Disposition", "")
    filename_match = re.search(
        r"filename\*?=(?:UTF-8''|\")?([^\";]+)", disposition, flags=re.IGNORECASE
    )
    if filename_match:
        raw_name = unquote(filename_match.group(1).strip().strip('"'))
    else:
        raw_name = unquote(Path(urlsplit(url).path).name) or "index"

    raw_name = raw_name.replace("\\", "_").replace("/", "_")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", raw_name).strip("._") or "object"
    cleaned = cleaned[:140]
    suffix = Path(cleaned).suffix.casefold()
    if not suffix:
        guessed = CONTENT_TYPE_EXTENSIONS.get(content_type)
        if not guessed:
            guessed = mimetypes.guess_extension(content_type, strict=False) or ".bin"
        cleaned += guessed
    return cleaned


class Collector:
    """Coordinate HTTP retrieval, immutable storage, state, and manifests."""

    def __init__(
        self,
        lake_root: Path,
        *,
        timeout_seconds: float = 30.0,
        max_bytes: int = 50 * 1024 * 1024,
        min_interval_seconds: float = 0.75,
        retries: int = 2,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.lake_root = lake_root.resolve()
        self.timeout_seconds = timeout_seconds
        self.max_bytes = max_bytes
        self.retries = retries
        self.user_agent = user_agent
        self.rate_limiter = RateLimiter(min_interval_seconds)
        self.state_path = self.lake_root / "metadata" / "collector_state.json"
        self.state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"version": 1, "objects_by_sha256": {}, "urls": {}}
        with self.state_path.open(encoding="utf-8") as handle:
            state = json.load(handle)
        if state.get("version") != 1:
            raise ValueError("Unsupported collector state version")
        state.setdefault("objects_by_sha256", {})
        state.setdefault("urls", {})
        if not isinstance(state["objects_by_sha256"], dict) or not isinstance(
            state["urls"], dict
        ):
            raise ValueError("Collector state object and URL indexes must be mappings")
        for checksum, relative_string in state["objects_by_sha256"].items():
            if not SHA256_PATTERN.fullmatch(str(checksum)):
                raise ValueError(f"Invalid checksum in collector state: {checksum!r}")
            object_path = validated_object_path(self.lake_root, str(relative_string))
            if object_path.is_file() and hashlib.sha256(object_path.read_bytes()).hexdigest() != checksum:
                raise ValueError(f"Collector state checksum mismatch: {object_path}")
        for cache_key, cached in state["urls"].items():
            if not isinstance(cached, dict):
                raise ValueError(f"Invalid URL cache entry in collector state: {cache_key!r}")
            relative_string = cached.get("object_path")
            if relative_string:
                validated_object_path(self.lake_root, str(relative_string))
        return state

    def _conditional_headers(self, url: str) -> dict[str, str]:
        cached = self.state["urls"].get(url, {})
        headers: dict[str, str] = {}
        if cached.get("etag"):
            headers["If-None-Match"] = cached["etag"]
        if cached.get("last_modified"):
            headers["If-Modified-Since"] = cached["last_modified"]
        return headers

    def _read_response(self, response: Any) -> bytes:
        declared = response.headers.get("Content-Length")
        if declared and int(declared) > self.max_bytes:
            raise ValueError(
                f"Content-Length {declared} exceeds limit {self.max_bytes} bytes"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = response.read(min(1024 * 1024, self.max_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > self.max_bytes:
                raise ValueError(f"Response exceeds limit {self.max_bytes} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    def _request(
        self,
        source: Source,
        url: str,
        method: str,
        request_body: str,
        *,
        use_conditional_headers: bool = True,
    ) -> tuple[Any, bytes]:
        cache_key = f"{method} {url}"
        headers = {
            "Accept": "text/html,application/pdf,application/json,application/xml,text/csv,*/*;q=0.5",
            "Accept-Encoding": "identity",
            "User-Agent": self.user_agent,
        }
        if use_conditional_headers:
            headers.update(self._conditional_headers(cache_key))
        headers.update(dict(source.request_headers))
        data = None
        if method == "POST":
            data = request_body.encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            headers["X-Requested-With"] = "XMLHttpRequest"
        request = Request(url, data=data, headers=headers, method=method)
        opener = build_opener(RestrictedRedirectHandler(source.allowed_hosts))

        for attempt in range(self.retries + 1):
            self.rate_limiter.wait(url)
            try:
                response = opener.open(request, timeout=self.timeout_seconds)
                try:
                    payload = self._read_response(response)
                finally:
                    response.close()
                return response, payload
            except HTTPError as error:
                if error.code == 304:
                    error.close()
                    return error, b""
                if error.code not in {408, 425, 429, 500, 502, 503, 504}:
                    raise
                if attempt >= self.retries:
                    raise
            except URLError:
                if attempt >= self.retries:
                    raise
            time.sleep(2**attempt)
        raise RuntimeError("Retry loop exited unexpectedly")

    def _store_object(
        self,
        source: Source,
        url: str,
        response: Any,
        data: bytes,
        retrieved_at: datetime,
    ) -> tuple[str, str, bool, str]:
        checksum = hashlib.sha256(data).hexdigest()
        content_type = media_type(response.headers.get("Content-Type"))
        cached_object = self.state["objects_by_sha256"].get(checksum)
        if cached_object:
            cached_path = validated_object_path(self.lake_root, str(cached_object))
            if cached_path.is_file():
                if hashlib.sha256(cached_path.read_bytes()).hexdigest() != checksum:
                    raise ValueError(f"Cached object checksum mismatch: {cached_path}")
                return checksum, cached_object, False, content_type

        basename = safe_basename(response.geturl(), response.headers, content_type)
        date = retrieved_at.strftime("%Y-%m-%d")
        relative = Path("raw") / "objects" / f"sha256={checksum[:2]}"
        relative = relative / f"{checksum}__{basename}"
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
            raise ValueError(f"Canonical object checksum mismatch: {destination}")

        relative_string = relative.as_posix()
        self.state["objects_by_sha256"][checksum] = relative_string
        return checksum, relative_string, True, content_type

    def fetch(
        self,
        source: Source,
        url: str,
        *,
        method: str = "GET",
        request_body: str = "",
        parent_url: str | None = None,
    ) -> FetchResult:
        """Fetch one URL and preserve unique content."""
        retrieved_at = utc_now()
        base = FetchResult(
            source_id=source.id,
            company=source.company,
            category=source.category,
            url=url,
            request_method=method,
            parent_url=parent_url,
            retrieved_at=isoformat(retrieved_at),
            status="error",
        )
        try:
            cache_key = f"{method} {url}"
            response, data = self._request(source, url, method, request_body)
            status = int(getattr(response, "status", response.code))
            if status == 304:
                cached = self.state["urls"].get(cache_key, {})
                cached_checksum = cached.get("checksum_sha256")
                cached_relative = cached.get("object_path")
                cached_valid = bool(
                    SHA256_PATTERN.fullmatch(str(cached_checksum or ""))
                    and cached_relative
                )
                if cached_valid:
                    cached_path = validated_object_path(
                        self.lake_root, str(cached_relative)
                    )
                    cached_valid = bool(
                        cached_path.is_file()
                        and hashlib.sha256(cached_path.read_bytes()).hexdigest()
                        == cached_checksum
                    )
                if not cached_valid:
                    response, data = self._request(
                        source,
                        url,
                        method,
                        request_body,
                        use_conditional_headers=False,
                    )
                    status = int(getattr(response, "status", response.code))
                else:
                    base.status = "not_modified"
                    base.http_status = 304
                    base.final_url = cached.get("final_url", url)
                    base.content_type = cached.get("content_type")
                    base.size_bytes = cached.get("size_bytes")
                    base.checksum_sha256 = cached_checksum
                    base.object_path = cached_relative
                    return base

            final_url = response.geturl()
            final_host = (urlsplit(final_url).hostname or "").casefold()
            if final_host not in source.allowed_hosts:
                raise ValueError(f"Final URL left allowed_hosts: {final_url}")

            checksum, object_path, created, content_type = self._store_object(
                source, url, response, data, retrieved_at
            )
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
            url_state = {
                "checksum_sha256": checksum,
                "object_path": object_path,
                "etag": etag,
                "last_modified": last_modified,
                "final_url": final_url,
                "content_type": content_type,
                "size_bytes": len(data),
                "last_seen_at": isoformat(retrieved_at),
            }
            previous = self.state["urls"].get(cache_key, {})
            self.state["urls"][cache_key] = url_state

            base.status = "fetched" if created else "unchanged"
            if previous.get("checksum_sha256") == checksum:
                base.status = "unchanged"
            base.http_status = status
            base.final_url = final_url
            base.content_type = content_type
            base.size_bytes = len(data)
            base.checksum_sha256 = checksum
            base.object_path = object_path
            base.etag = etag
            base.last_modified = last_modified
            base.data = data
            return base
        except (HTTPError, URLError, OSError, ValueError) as error:
            base.error = f"{type(error).__name__}: {error}"
            if isinstance(error, HTTPError):
                base.http_status = error.code
            return base

    def collect_source(self, source: Source) -> list[FetchResult]:
        """Collect one seed page and its direct document links."""
        results = [
            self.fetch(
                source,
                source.url,
                method=source.method,
                request_body=source.request_body,
            )
        ]
        seed = results[0]
        if not source.discover_documents or not seed.data:
            return results
        if media_type(seed.content_type) not in {
            "text/html",
            "application/json",
            "text/json",
            "text/plain",
        }:
            return results

        for url in discover_document_urls(seed.data, seed.final_url or source.url, source):
            results.append(self.fetch(source, url, parent_url=source.url))
        return results

    def run(self, sources: list[Source], config_path: Path) -> dict[str, Any]:
        """Serialize collection runs so state updates cannot overwrite each other."""
        lock_path = self.lake_root / "metadata" / "collector.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                self.state = self._load_state()
                return self._run_locked(sources, config_path)
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _run_locked(self, sources: list[Source], config_path: Path) -> dict[str, Any]:
        """Run selected sources and atomically publish state and manifest."""
        started = utc_now()
        run_id = f"{started.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        results: list[FetchResult] = []
        for source in sources:
            results.extend(self.collect_source(source))

        finished = utc_now()
        counts: dict[str, int] = {}
        for result in results:
            counts[result.status] = counts.get(result.status, 0) + 1
        manifest = {
            "manifest_version": 1,
            "run_id": run_id,
            "started_at": isoformat(started),
            "finished_at": isoformat(finished),
            "config_path": str(config_path.resolve()),
            "lake_root": str(self.lake_root),
            "source_count": len(sources),
            "request_count": len(results),
            "status_counts": counts,
            "results": [result.manifest_mapping() for result in results],
        }
        atomic_json_write(self.state_path, self.state)
        manifest_path = self.lake_root / "metadata" / "manifests_json"
        manifest_path /= started.strftime("%Y/%m/%d")
        manifest_path /= f"{run_id}.json"
        atomic_json_write(manifest_path, manifest)
        manifest["manifest_path"] = str(manifest_path)
        return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect immutable snapshots from configured public data sources."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Collect only this source id; may be repeated.",
    )
    parser.add_argument("--list-sources", action="store_true")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-megabytes", type=int, default=50)
    parser.add_argument("--min-interval", type=float, default=0.75)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        sources = load_sources(args.config)
        if args.list_sources:
            for source in sources:
                print(f"{source.id:28} {source.company:16} {source.url}")
            return 0

        if args.source:
            requested = set(args.source)
            unknown = sorted(requested - {source.id for source in sources})
            if unknown:
                raise ValueError(f"Unknown source ids: {', '.join(unknown)}")
            sources = [source for source in sources if source.id in requested]

        collector = Collector(
            args.lake_root,
            timeout_seconds=args.timeout,
            max_bytes=args.max_megabytes * 1024 * 1024,
            min_interval_seconds=args.min_interval,
            retries=args.retries,
            user_agent=args.user_agent,
        )
        manifest = collector.run(sources, args.config)
        print(f"Run: {manifest['run_id']}")
        print(f"Sources: {manifest['source_count']}")
        print(f"Requests: {manifest['request_count']}")
        print(f"Statuses: {json.dumps(manifest['status_counts'], sort_keys=True)}")
        print(f"Manifest: {manifest['manifest_path']}")
        return 1 if manifest["status_counts"].get("error") == manifest["request_count"] else 0
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
