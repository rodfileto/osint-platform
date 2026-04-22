# Flyway Migrations

This directory stores versioned PostgreSQL schema changes managed by Flyway.

## Current strategy

- Flyway owns both initial schema creation and future schema modifications.
- Empty database: `flyway migrate`
- Existing database: `flyway migrate`
- Every change must be a new `V...__description.sql` file in one of the module folders below.

## Folder layout

- `cnpj/` for CNPJ schema changes
- `finep/` for FINEP schema changes
- `geo/` for canonical geography schemas, boundaries, and cross-dataset mapping objects
- `inovalink/` for Inovalink schema changes and ingestion support objects

## Naming convention

Use globally unique versions across all folders because Flyway shares one version history table.

Recommended format:

`V2026.03.07.001__cnpj_add_socio_updated_at.sql`

Examples:

- `V2026.03.07.001__cnpj_add_company_search_index.sql`
- `V2026.03.08.001__finep_add_contract_status_column.sql`
- `V2026.03.15.001__geo_enable_postgis.sql`

## Commands

Run migrations:

```bash
./dev/scripts/run-flyway.sh migrate
# or, for the prod-like stack
./infrastructure/postgres/run-flyway.sh --env prod-like --yes migrate
```

Show migration status:

```bash
./dev/scripts/run-flyway.sh info
# or
./infrastructure/postgres/run-flyway.sh --env prod-like info
```

Validate migration files:

```bash
./dev/scripts/run-flyway.sh validate
# or
./infrastructure/postgres/run-flyway.sh --env prod-like validate
```

## Notes

- Do not edit old migrations after they have been applied anywhere outside your local machine.
- Prefer roll-forward fixes: add a new migration instead of modifying an old one.
- For PostgreSQL statements that cannot run inside a transaction, place them in their own migration file and handle them intentionally.