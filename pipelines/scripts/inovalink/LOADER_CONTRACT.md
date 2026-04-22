# Inovalink Loader Contract

This document defines the loader contract for the Inovalink Airflow pipeline.

The goal is to make the DAG implementation deterministic, idempotent, and easy to evolve while preserving full raw payload fidelity.

## Scope

The loader reads the public Livewire response captured from `https://www.inovalink.org/livewire/update` and populates these PostgreSQL objects:

- `inovalink.sync_manifest`
- `inovalink.component_payload`
- `inovalink.organization_snapshot`
- `inovalink.organization_relationship`
- `inovalink.organization_cnpj_match`

The loader must not depend on a source-provided snapshot date, because the current endpoint does not expose one explicitly.

## DAG Shape

Recommended DAG id: `inovalink_load_postgres`

Recommended task order:

1. `collect_livewire_payload`
2. `register_sync_manifest`
3. `store_component_payload`
4. `load_organization_snapshot`
5. `load_organization_relationship`
6. `seed_organization_cnpj_match`
7. `resolve_exact_cnpj_matches`
8. `mark_manifest_loaded`
9. `generate_report`

Optional future tasks:

1. `resolve_cnpj_fuzzy`
2. `materialize_latest_views`

## Input Contract

The collection task must return one JSON-serializable payload with this shape:

```json
{
  "entrypoint_url": "https://www.inovalink.org/",
  "request_url": "https://www.inovalink.org/livewire/update",
  "request_method": "POST",
  "component_name": "site.map",
  "component_id": "WHHpIiouhfe5xjn7o3l1",
  "collected_at": "2026-04-17T23:10:59Z",
  "response_status_code": 200,
  "response_content_type": "application/json",
  "response_content_encoding": "gzip",
  "response_size_bytes": 6033986,
  "response_checksum_sha256": "...",
  "http_etag": null,
  "http_last_modified": null,
  "collection_counts": {
    "incubators": 231,
    "accelerators": 40,
    "operationParks": 77,
    "implantationParks": 39,
    "planningParks": 10,
    "incubatedCompanies": 93,
    "acceleratedCompanies": 11,
    "graduatedCompanies": 48,
    "residentCompanies": 4084
  },
  "raw_request_path": "s3://osint-raw/inovalink/livewire/20260417/request.json",
  "raw_response_path": "s3://osint-raw/inovalink/livewire/20260417/response.json",
  "local_request_path": "/opt/airflow/data/inovalink/artifacts/20260417/request.json",
  "local_response_path": "/opt/airflow/data/inovalink/artifacts/20260417/response.json",
  "component_snapshot": {},
  "component_effects": {},
  "component_html": "<section ...>",
  "actors": {
    "incubators": [],
    "accelerators": [],
    "operationParks": [],
    "implantationParks": [],
    "planningParks": [],
    "incubatedCompanies": [],
    "acceleratedCompanies": [],
    "graduatedCompanies": [],
    "residentCompanies": []
  }
}
```

`actors` must already be decoded from `effects.xjs[0].params[0]`.

Artifact persistence rules:

1. `raw_request_path` and `raw_response_path` should point to MinIO-backed S3 URIs.
2. `local_request_path` and `local_response_path` may still be returned for local debugging and replay.
3. The raw request and response artifacts must preserve the exact JSON sent to and received from Livewire.

## Manifest Contract

Task: `register_sync_manifest`

Input:

- collection payload

Output:

```json
{
  "manifest_id": 123,
  "processing_status": "collected",
  "collected_at": "2026-04-17T23:10:59Z"
}
```

DB rules:

- Insert one row into `inovalink.sync_manifest`.
- `snapshot_date` stays `NULL` unless a future collector derives it from a trusted external signal.
- `records_total` is the sum of all collection counts.
- `processing_status` starts as `collected`.

Idempotency:

- No hard dedupe requirement for manifests.
- Multiple runs with the same payload are allowed and represent independent collection executions.

## Component Payload Contract

Task: `store_component_payload`

Input:

- collection payload
- `manifest_id`

Output:

```json
{
  "manifest_id": 123,
  "component_name": "site.map",
  "payload_row_count": 4633
}
```

DB rules:

- Insert one row into `inovalink.component_payload`.
- `component_snapshot` stores the decoded snapshot JSON.
- `component_effects` stores the decoded effects JSON.
- `component_html` stores the returned HTML fragment.
- `payload_row_count` is the total number of actor objects across all collections.

## Organization Snapshot Contract

Task: `load_organization_snapshot`

Input:

- collection payload
- `manifest_id`

Output:

```json
{
  "manifest_id": 123,
  "rows_inserted": 4633,
  "rows_by_collection": {
    "incubators": 231,
    "accelerators": 40,
    "operationParks": 77,
    "implantationParks": 39,
    "planningParks": 10,
    "incubatedCompanies": 93,
    "acceleratedCompanies": 11,
    "graduatedCompanies": 48,
    "residentCompanies": 4084
  }
}
```

Collection to entity mapping:

| Source collection | `entity_kind` | Notes |
| --- | --- | --- |
| `incubators` | `incubator` | Parent actors |
| `accelerators` | `accelerator` | Parent actors |
| `operationParks` | `park` | Category-specific park subtype |
| `implantationParks` | `park` | Category-specific park subtype |
| `planningParks` | `park` | Category-specific park subtype |
| `incubatedCompanies` | `company` | Child actors |
| `acceleratedCompanies` | `company` | Child actors |
| `graduatedCompanies` | `company` | Child actors |
| `residentCompanies` | `company` | Child actors |

Normalization rules:

1. `source_record_id` comes from source field `id`.
2. `source_linkable_type` comes from `linkable_type`.
3. `source_linkable_id` comes from `linkable_id`.
4. `city_ibge_code` comes from `address.city.id`, left-padded to 7 if needed.
5. `state_ibge_code` comes from `address.city.state.id`, left-padded to 2 if needed.
6. `state_abbrev` comes from `address.city.state.uf`.
7. `latitude` and `longitude` are parsed as numeric when valid.
8. `raw_payload` stores the full source object unchanged.

Idempotency:

- Use `ON CONFLICT (manifest_id, source_collection, source_record_id) DO UPDATE`.
- Re-running the same manifest should be safe.

## Relationship Contract

Task: `load_organization_relationship`

Input:

- `manifest_id`
- already loaded `organization_snapshot` rows for the same manifest

Output:

```json
{
  "manifest_id": 123,
  "rows_inserted": 152,
  "rows_unresolved": 0
}
```

Resolution rules:

1. Build an in-memory map of `(source_collection, source_record_id) -> organization_snapshot.id` for the current manifest.
2. For every organization with both `source_linkable_type` and `source_linkable_id`, derive the parent source collection.
3. Resolve the parent row from the same manifest.

Model class to parent collection mapping:

| `linkable_type` | Parent source collection |
| --- | --- |
| `App\\Models\\Incubator` | `incubators` |
| `App\\Models\\Accelerator` | `accelerators` |
| `App\\Models\\Park` | `operationParks`, `implantationParks`, or `planningParks` |

Relationship type mapping:

| Child source collection | Relationship type |
| --- | --- |
| `incubatedCompanies` | `incubated_by` |
| `acceleratedCompanies` | `accelerated_by` |
| `graduatedCompanies` | `graduated_from` |
| `residentCompanies` | `resident_in` |
| any unresolved future pattern | `linked_to` |

Park resolution rule:

- For `App\\Models\\Park`, search the three park collections by `source_record_id = linkable_id`.
- There should be at most one match inside a single manifest.
- If none is found, keep `parent_organization_id = NULL` and still insert the row with upstream identifiers.

Idempotency:

- Use the unique constraint on `(manifest_id, child_organization_id, relationship_type, parent_model_class, parent_source_record_id)`.

## CNPJ Match Seed Contract

Task: `seed_organization_cnpj_match`

Input:

- `manifest_id`
- already loaded `organization_snapshot` rows

Output:

```json
{
  "manifest_id": 123,
  "rows_seeded": 4236,
  "rows_with_source_cnpj": 4219
}
```

Rules:

1. Insert one row in `inovalink.organization_cnpj_match` for every organization snapshot row.
2. Copy `organization_snapshot.cnpj` into `source_cnpj`.
3. If the CNPJ is 14 digits after normalization, split into:
   - `cnpj_basico` = first 8
   - `cnpj_ordem` = next 4
   - `cnpj_dv` = last 2
   - `cnpj_14` = normalized full value
4. Initial `match_status`:
   - `pending` for valid or empty CNPJ
   - `invalid` for malformed non-empty CNPJ
5. Do not attempt fuzzy matching in this task.

Idempotency:

- Use `ON CONFLICT (organization_snapshot_id) DO UPDATE`.

## Exact CNPJ Match Contract

Task: `resolve_exact_cnpj_matches`

Input:

- `manifest_id`
- previously seeded rows in `inovalink.organization_cnpj_match`

Output:

```json
{
  "manifest_id": 123,
  "matched_rows": 4187,
  "unmatched_rows": 32
}
```

Rules:

1. Only attempt exact resolution for rows with normalized `cnpj_14` and `match_status IN ('pending', 'unmatched')`.
2. Match against `cnpj.estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv)`.
3. When the establishment exists, set:
   - `match_status = 'matched'`
   - `match_method = 'exact_cnpj_estabelecimento'`
   - `match_confidence = 1.0`
   - `matched_at = now()`
   - `matched_empresa_cnpj_basico`
   - `matched_estab_cnpj_basico`
   - `matched_estab_cnpj_ordem`
   - `matched_estab_cnpj_dv`
4. Persist JSON evidence showing at least the organization name and normalized `cnpj_14` used for the exact attempt.
5. When no establishment match exists, mark the row as `unmatched` and retain evidence that the exact attempt was performed.
6. This task does not perform fuzzy matching or manual review routing.

Idempotency:

- Re-running the task for the same manifest is safe.
- Matched rows should remain matched unless a future task intentionally overrides them with a stronger reviewed outcome.

## Manifest Finalization Contract

Task: `mark_manifest_loaded`

Input:

- `manifest_id`
- task summaries from snapshot, relationship, cnpj seed, and exact cnpj match steps

Output:

```json
{
  "manifest_id": 123,
  "processing_status": "loaded"
}
```

Rules:

- Set `processing_status = 'loaded'` only after all write tasks succeed.
- Set `processing_status = 'failed'` and store `error_message` on failure handling path.

## Failure Handling

If any task after manifest registration fails:

1. Update `inovalink.sync_manifest.processing_status = 'failed'`
2. Persist the exception message in `error_message`
3. Do not delete already inserted snapshot rows unless the task explicitly wraps everything in one transaction

Preferred approach:

- Each task owns its own transaction boundary.
- The manifest row is the authoritative record of partial failure.

## Python Module Contract

Recommended module: `pipelines/scripts/inovalink/loader.py`

Recommended functions:

```python
def register_sync_manifest(collection_payload: dict) -> dict: ...
def store_component_payload(manifest_id: int, collection_payload: dict) -> dict: ...
def load_organization_snapshot(manifest_id: int, actors: dict[str, list[dict]]) -> dict: ...
def load_organization_relationship(manifest_id: int) -> dict: ...
def seed_organization_cnpj_match(manifest_id: int) -> dict: ...
def resolve_exact_cnpj_matches(manifest_id: int) -> dict: ...
def mark_manifest_loaded(manifest_id: int) -> dict: ...
def mark_manifest_failed(manifest_id: int, error_message: str) -> None: ...
```

Recommended collection helper:

```python
def extract_livewire_actor_payload(response_json: dict) -> dict[str, list[dict]]: ...
```

## Data Quality Expectations

Expected realities from the current source:

1. `snapshot_date` is absent.
2. `city_ibge_code` behaves like IBGE municipality code.
3. `linkable_type` and `linkable_id` encode real parent-child relationships.
4. Some organizations may have empty or malformed CNPJ.
5. Some fields such as `site`, `instagram`, and `linkedin` can be null or empty strings.
6. Raw request and response artifacts should be recoverable from MinIO for audit and replay.

The loader must preserve source truth even when values are incomplete.

## Done Criteria

The loader contract is satisfied when:

1. One successful collection creates one `sync_manifest` row.
2. The decoded component payload is stored once in `component_payload`.
3. All actor collections are normalized into `organization_snapshot`.
4. All `linkable_*` edges are materialized in `organization_relationship`.
5. All organizations are seeded into `organization_cnpj_match`.
6. Exact establishment-level CNPJ resolution is attempted for every normalized `cnpj_14` candidate.
7. The manifest is promoted from `collected` to `loaded` only after all previous steps succeed.