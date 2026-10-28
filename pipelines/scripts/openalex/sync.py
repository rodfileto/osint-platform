"""Mirror the OpenAlex Parquet snapshot into bronze (MinIO), keeping only the latest release.

OpenAlex replaces its bucket in place each quarter, so the manifest, the listing and the files can
disagree while a release is being written. A sync only starts when every manifest file is listed
upstream, every downloaded file must match the manifest size, and stale files of the previous
release are removed only after every file of the new one is in. manifest.json is written to bronze
last: its presence means the release is complete.
"""

import http.client
import json
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from . import state, storage
from .config import EXCLUDED_ENTITIES, UPSTREAM_PREFIX, Lake

S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
CHUNK = 8 * 1024 * 1024
MANIFEST_FILE = "manifest.json"


@dataclass(frozen=True)
class FileSpec:
    key: str
    size: int
    records: int
    etag: str = ""

    @property
    def rel_path(self) -> str:
        return self.key.removeprefix(UPSTREAM_PREFIX)

    @property
    def entity(self) -> str:
        return self.rel_path.split("/", 1)[0]


def http_get(url: str, headers: dict | None = None, timeout: int = 60):
    return urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=timeout)


def fetch_manifest(upstream_url: str) -> dict:
    with http_get(f"{upstream_url}/{UPSTREAM_PREFIX}{MANIFEST_FILE}") as r:
        return json.loads(r.read())


def parse_listing(xml_bytes: bytes) -> tuple[dict[str, str], str | None]:
    root = ET.fromstring(xml_bytes)
    etags = {
        c.findtext(f"{S3_NS}Key"): c.findtext(f"{S3_NS}ETag").strip('"')
        for c in root.iter(f"{S3_NS}Contents")
    }
    token = root.findtext(f"{S3_NS}NextContinuationToken")
    return etags, token


def list_etags(upstream_url: str, prefix: str) -> dict[str, str]:
    etags, token = {}, None
    while True:
        params = {"list-type": "2", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        with http_get(f"{upstream_url}/?{urllib.parse.urlencode(params)}") as r:
            page, token = parse_listing(r.read())
        etags.update(page)
        if not token:
            return etags


def manifest_files(manifest: dict, excluded: set[str] = EXCLUDED_ENTITIES) -> list[FileSpec]:
    files = []
    for ent in manifest["entities"]:
        if ent["entity"] in excluded:
            continue
        for f in ent["files"]:
            key = f["url"].removeprefix("s3://openalex/")
            files.append(FileSpec(key, f["meta"]["content_length"], f["meta"]["record_count"]))
    return files


def plan(files: list[FileSpec], present: dict[str, tuple[int, str]]) -> list[FileSpec]:
    """Files whose bronze copy is missing or has another size or source ETag."""
    return [f for f in files if present.get(f.rel_path) != (f.size, f.etag)]


def stale_keys(present: set[str], files: list[FileSpec]) -> list[str]:
    """Bronze files of entities still synced that the release no longer has."""
    keep = {f.rel_path for f in files}
    return sorted(key for key in present if key not in keep)


def download(f: FileSpec, upstream_url: str, staging: Path, retries: int = 6) -> Path:
    """Download one file to staging, resuming a partial download; returns the finished file."""
    dest = staging / f.rel_path
    part = dest.with_name(dest.name + ".part")
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{upstream_url}/{urllib.parse.quote(f.key)}"
    for attempt in range(retries):
        try:
            have = part.stat().st_size if part.exists() else 0
            if have > f.size:
                part.unlink()
                have = 0
            if have < f.size:
                headers = {"Range": f"bytes={have}-"} if have else {}
                with http_get(url, headers, timeout=120) as r:
                    mode = "ab" if r.status == 206 else "wb"
                    with open(part, mode) as out:
                        while chunk := r.read(CHUNK):
                            out.write(chunk)
            if part.stat().st_size != f.size:
                raise OSError(f"size mismatch {part.stat().st_size} != {f.size}")
            part.replace(dest)
            return dest
        except (OSError, http.client.HTTPException):
            if attempt == retries - 1:
                raise
            time.sleep(2**attempt)


def _transfer(f: FileSpec, lake: Lake, s3) -> FileSpec:
    local = download(f, lake.upstream_url, lake.staging)
    key = lake.object_key(f.rel_path)
    s3.upload_file(str(local), lake.bucket, key)
    size = s3.head_object(Bucket=lake.bucket, Key=key)["ContentLength"]
    local.unlink()
    if size != f.size:
        raise OSError(f"{key}: {size} bytes in MinIO, manifest says {f.size}")
    return f


def upstream_release(lake: Lake) -> str:
    return fetch_manifest(lake.upstream_url)["date"]


def sync(lake: Lake, workers: int = 8, dry_run: bool = False, log=print) -> bool:
    manifest = fetch_manifest(lake.upstream_url)
    files = manifest_files(manifest)
    etags = {}
    for ent in sorted({f.entity for f in files}):
        etags.update(list_etags(lake.upstream_url, f"{UPSTREAM_PREFIX}{ent}/"))
    unlisted = [f.key for f in files if f.key not in etags]
    if unlisted:
        log(f"release {manifest['date']}: {len(unlisted)} manifest files are not listed upstream "
            f"(e.g. {unlisted[0]}); the bucket is probably being replaced, try again later")
        return False
    files = [FileSpec(f.key, f.size, f.records, etags[f.key]) for f in files]

    s3 = storage.s3_client()
    conn = state.connect()
    try:
        storage.ensure_bucket(s3, lake.bucket)
        objects = storage.list_objects(s3, lake.bucket, lake.prefix)
        recorded = state.bronze_files(conn)
        # A file counts as present only if MinIO has it with the recorded size.
        present = {k: v for k, v in recorded.items() if objects.get(lake.object_key(k)) == v[0]}
        todo = plan(files, present)
        stale = stale_keys(set(recorded) | {k.removeprefix(lake.prefix + "/") for k in objects}
                           - {MANIFEST_FILE}, files)
        total_gb = sum(f.size for f in todo) / 1e9
        log(f"release {manifest['date']}: {len(files)} files, {len(todo)} to download "
            f"({total_gb:.1f} GB), {len(stale)} stale to remove")
        if dry_run:
            return True

        s3.delete_object(Bucket=lake.bucket, Key=lake.object_key(MANIFEST_FILE))
        failed, done_bytes, start = [], 0, time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_transfer, f, lake, s3): f for f in todo}
            for i, fut in enumerate(as_completed(futures), 1):
                f = futures[fut]
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001 - one failed file must not stop the others
                    failed.append(f)
                    log(f"FAILED {f.key}: {e}")
                    continue
                state.record_bronze(conn, [f], manifest["date"])
                done_bytes += f.size
                rate = done_bytes / 1e6 / max(time.time() - start, 1)
                log(f"[{i}/{len(todo)}] {done_bytes / 1e9:.1f}/{total_gb:.1f} GB "
                    f"{rate:.0f} MB/s {f.rel_path}")
        if failed:
            log(f"{len(failed)} files failed; the next run retries them")
            return False

        storage.delete_objects(s3, lake.bucket, [lake.object_key(k) for k in stale])
        state.forget_bronze(conn, stale)
        s3.put_object(Bucket=lake.bucket, Key=lake.object_key(MANIFEST_FILE),
                      Body=json.dumps(manifest).encode())
        state.mark_release(conn, manifest["date"], "synced_at")
        log(f"release {manifest['date']} complete in bronze")
        return True
    finally:
        conn.close()
