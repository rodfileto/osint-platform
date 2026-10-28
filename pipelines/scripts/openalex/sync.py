"""Mirror the OpenAlex parquet snapshot into bronze, keeping only the latest release."""

import http.client
import json
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .config import BUCKET_URL, EXCLUDED_ENTITIES, PREFIX

S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
CHUNK = 8 * 1024 * 1024
STATE_FILE = "_state.json"
MANIFEST_FILE = "manifest.json"


@dataclass(frozen=True)
class FileSpec:
    key: str
    size: int
    records: int
    etag: str = ""

    @property
    def rel_path(self) -> str:
        return self.key.removeprefix(PREFIX)


def http_get(url: str, headers: dict | None = None, timeout: int = 60):
    return urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=timeout)


def fetch_manifest() -> dict:
    with http_get(f"{BUCKET_URL}/{PREFIX}manifest.json") as r:
        return json.loads(r.read())


def parse_listing(xml_bytes: bytes) -> tuple[dict[str, str], str | None]:
    root = ET.fromstring(xml_bytes)
    etags = {
        c.findtext(f"{S3_NS}Key"): c.findtext(f"{S3_NS}ETag").strip('"')
        for c in root.iter(f"{S3_NS}Contents")
    }
    token = root.findtext(f"{S3_NS}NextContinuationToken")
    return etags, token


def list_etags(prefix: str) -> dict[str, str]:
    etags, token = {}, None
    while True:
        params = {"list-type": "2", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        with http_get(f"{BUCKET_URL}/?{urllib.parse.urlencode(params)}") as r:
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


def plan(files: list[FileSpec], root: Path, state: dict[str, str]) -> list[FileSpec]:
    todo = []
    for f in files:
        dest = root / f.rel_path
        if dest.exists() and dest.stat().st_size == f.size and state.get(f.key) == f.etag:
            continue
        todo.append(f)
    return todo


def stale_paths(root: Path, files: list[FileSpec]) -> list[Path]:
    keep = {root / f.rel_path for f in files}
    entities = {f.rel_path.split("/", 1)[0] for f in files}
    stale = []
    for ent in entities:
        for p in (root / ent).rglob("*"):
            if p.is_file() and p not in keep:
                stale.append(p)
    return stale


def download(f: FileSpec, root: Path, retries: int = 6) -> None:
    dest = root / f.rel_path
    part = dest.with_name(dest.name + ".part")
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BUCKET_URL}/{urllib.parse.quote(f.key)}"
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
            return
        except (OSError, http.client.HTTPException):
            if attempt == retries - 1:
                raise
            time.sleep(2**attempt)


def _load_state(root: Path) -> dict[str, str]:
    p = root / STATE_FILE
    return json.loads(p.read_text()) if p.exists() else {}


def _save_state(root: Path, state: dict[str, str]) -> None:
    tmp = root / (STATE_FILE + ".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(root / STATE_FILE)


def sync(root: Path, workers: int = 8, dry_run: bool = False, log=print) -> bool:
    root.mkdir(parents=True, exist_ok=True)
    manifest = fetch_manifest()
    files = manifest_files(manifest)
    entities = sorted({f.rel_path.split("/", 1)[0] for f in files})
    etags = {}
    for ent in entities:
        etags.update(list_etags(f"{PREFIX}{ent}/"))
    files = [FileSpec(f.key, f.size, f.records, etags.get(f.key, "")) for f in files]

    state = _load_state(root)
    todo = plan(files, root, state)
    stale = stale_paths(root, files)
    total_gb = sum(f.size for f in todo) / 1e9
    log(f"release {manifest['date']}: {len(files)} files, {len(todo)} to download "
        f"({total_gb:.1f} GB), {len(stale)} stale to remove")
    if dry_run:
        return True

    (root / MANIFEST_FILE).unlink(missing_ok=True)
    for p in stale:
        p.unlink()

    lock = threading.Lock()
    done_bytes, failed, start = 0, [], time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(download, f, root): f for f in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            f = futures[fut]
            try:
                fut.result()
            except (OSError, http.client.HTTPException) as e:
                failed.append(f)
                log(f"FAILED {f.key}: {e}")
                continue
            with lock:
                state[f.key] = f.etag
                done_bytes += f.size
                if i % 10 == 0 or i == len(todo):
                    _save_state(root, state)
            rate = done_bytes / 1e6 / max(time.time() - start, 1)
            log(f"[{i}/{len(todo)}] {done_bytes / 1e9:.1f}/{total_gb:.1f} GB "
                f"{rate:.0f} MB/s {f.rel_path}")

    state = {f.key: state[f.key] for f in files if f.key in state}
    _save_state(root, state)
    if failed:
        log(f"{len(failed)} files failed; rerun `lake sync` to retry")
        return False
    (root / MANIFEST_FILE).write_text(json.dumps(manifest))
    log(f"release {manifest['date']} complete")
    return True
