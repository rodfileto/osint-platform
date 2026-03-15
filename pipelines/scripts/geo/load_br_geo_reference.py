#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass

import geobr
import psycopg2
from psycopg2.extras import execute_values


GEOBR_YEAR = int(os.getenv("GEOBR_YEAR", "2019"))
GEOBR_SIMPLIFIED = os.getenv("GEOBR_SIMPLIFIED", "true").strip().lower() in {"1", "true", "yes", "on"}
SOURCE_NAME = "geobr"
MAPPING_SOURCE = "shared-geobr-loader"


@dataclass(frozen=True)
class CnpjMunicipalityContext:
    cnpj_municipality_code: str
    cnpj_municipality_name: str | None
    inferred_state_abbrev: str | None
    state_count: int


def log(message: str) -> None:
    print(f"[geo-loader] {message}", flush=True)


def connect_postgres():
    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    connection.autocommit = True
    return connection


def resolve_column(frame, candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    available = ", ".join(str(column) for column in frame.columns)
    raise RuntimeError(f"Missing expected columns {candidates}. Available columns: {available}")


def normalize_code(value, width: int) -> str:
    if value is None:
        raise RuntimeError("Encountered empty geography code in geobr dataset.")

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, float):
        if not value.is_integer():
            raise RuntimeError(f"Encountered non-integer geography code: {value}")
        text = str(int(value))
    elif isinstance(value, int):
        text = str(value)
    else:
        text = re.sub(r"\D", "", str(value).strip())

    if not text:
        raise RuntimeError("Encountered empty geography code in geobr dataset.")
    return text.zfill(width)


def normalize_name(value: str | None) -> str:
    text = (value or "").strip().upper()
    text = "".join(
        char for char in unicodedata.normalize("NFD", text)
        if unicodedata.category(char) != "Mn"
    )
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_scalar(connection, query: str, params: tuple | None = None) -> int:
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
        if row is None:
            return 0
        return int(row[0])


def should_refresh_states(connection) -> bool:
    count = fetch_scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM geo.br_state
        WHERE geobr_year = %s AND is_simplified = %s
        """,
        (GEOBR_YEAR, GEOBR_SIMPLIFIED),
    )
    return count < 27


def should_refresh_municipalities(connection) -> bool:
    count = fetch_scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM geo.br_municipality
        WHERE geobr_year = %s AND is_simplified = %s
        """,
        (GEOBR_YEAR, GEOBR_SIMPLIFIED),
    )
    return count < 5500


def read_states_frame():
    return geobr.read_state(code_state="all", year=GEOBR_YEAR, simplified=GEOBR_SIMPLIFIED)


def read_municipalities_frame():
    return geobr.read_municipality(code_muni="all", year=GEOBR_YEAR, simplified=GEOBR_SIMPLIFIED)


def upsert_states(connection) -> None:
    if not should_refresh_states(connection):
        log(f"State reference already loaded for year={GEOBR_YEAR} simplified={GEOBR_SIMPLIFIED}. Skipping download.")
        return

    states = read_states_frame()
    code_column = resolve_column(states, ["code_state", "code_state_ibge"])
    abbrev_column = resolve_column(states, ["abbrev_state", "state_abbrev"])
    name_column = resolve_column(states, ["name_state", "state_name"])

    rows: list[tuple] = []
    for record in states.itertuples(index=False):
        geometry = getattr(record, "geometry")
        rows.append(
            (
                normalize_code(getattr(record, code_column), 2),
                str(getattr(record, abbrev_column)).strip().upper(),
                str(getattr(record, name_column)).strip(),
                GEOBR_YEAR,
                GEOBR_SIMPLIFIED,
                SOURCE_NAME,
                None,
                geometry.wkb_hex,
            )
        )

    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO geo.br_state (
                state_ibge_code,
                state_abbrev,
                state_name,
                geobr_year,
                is_simplified,
                source_name,
                source_version,
                geom
            ) VALUES %s
            ON CONFLICT (state_ibge_code) DO UPDATE SET
                state_abbrev = EXCLUDED.state_abbrev,
                state_name = EXCLUDED.state_name,
                geobr_year = EXCLUDED.geobr_year,
                is_simplified = EXCLUDED.is_simplified,
                source_name = EXCLUDED.source_name,
                source_version = EXCLUDED.source_version,
                geom = EXCLUDED.geom
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, ST_Multi(ST_GeomFromWKB(decode(%s, 'hex'), 4674)))",
            page_size=100,
        )
        cursor.execute(
            """
            UPDATE geo.br_state
            SET
                geometry_srid = ST_SRID(geom),
                centroid = ST_PointOnSurface(geom),
                area_km2 = ST_Area(geom::geography) / 1000000.0
            WHERE geobr_year = %s AND is_simplified = %s
            """,
            (GEOBR_YEAR, GEOBR_SIMPLIFIED),
        )

    log(f"Upserted {len(rows)} state geometries.")


def upsert_municipalities(connection) -> None:
    if not should_refresh_municipalities(connection):
        log(f"Municipality reference already loaded for year={GEOBR_YEAR} simplified={GEOBR_SIMPLIFIED}. Skipping download.")
        return

    municipalities = read_municipalities_frame()
    municipality_code_column = resolve_column(municipalities, ["code_muni", "municipality_ibge_code"])
    municipality_name_column = resolve_column(municipalities, ["name_muni", "municipality_name"])
    state_abbrev_column = resolve_column(municipalities, ["abbrev_state", "state_abbrev"])
    state_code_column = resolve_column(municipalities, ["code_state", "state_ibge_code"])

    rows: list[tuple] = []
    for record in municipalities.itertuples(index=False):
        geometry = getattr(record, "geometry")
        rows.append(
            (
                normalize_code(getattr(record, municipality_code_column), 7),
                normalize_code(getattr(record, state_code_column), 2),
                str(getattr(record, state_abbrev_column)).strip().upper(),
                str(getattr(record, municipality_name_column)).strip(),
                GEOBR_YEAR,
                GEOBR_SIMPLIFIED,
                SOURCE_NAME,
                None,
                geometry.wkb_hex,
            )
        )

    with connection.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO geo.br_municipality (
                municipality_ibge_code,
                state_ibge_code,
                state_abbrev,
                municipality_name,
                geobr_year,
                is_simplified,
                source_name,
                source_version,
                geom
            ) VALUES %s
            ON CONFLICT (municipality_ibge_code) DO UPDATE SET
                state_ibge_code = EXCLUDED.state_ibge_code,
                state_abbrev = EXCLUDED.state_abbrev,
                municipality_name = EXCLUDED.municipality_name,
                geobr_year = EXCLUDED.geobr_year,
                is_simplified = EXCLUDED.is_simplified,
                source_name = EXCLUDED.source_name,
                source_version = EXCLUDED.source_version,
                geom = EXCLUDED.geom
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, ST_Multi(ST_GeomFromWKB(decode(%s, 'hex'), 4674)))",
            page_size=250,
        )
        cursor.execute(
            """
            UPDATE geo.br_municipality
            SET
                geometry_srid = ST_SRID(geom),
                centroid = ST_PointOnSurface(geom),
                area_km2 = ST_Area(geom::geography) / 1000000.0
            WHERE geobr_year = %s AND is_simplified = %s
            """,
            (GEOBR_YEAR, GEOBR_SIMPLIFIED),
        )

    log(f"Upserted {len(rows)} municipality geometries.")


def load_cnpj_municipality_contexts(connection) -> list[CnpjMunicipalityContext]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                municipality.codigo,
                municipality.nome,
                CASE
                    WHEN COUNT(DISTINCT establishment.uf) FILTER (WHERE establishment.uf IS NOT NULL AND establishment.uf <> '') = 1
                    THEN MAX(establishment.uf) FILTER (WHERE establishment.uf IS NOT NULL AND establishment.uf <> '')
                    ELSE NULL
                END AS inferred_state_abbrev,
                COUNT(DISTINCT establishment.uf) FILTER (WHERE establishment.uf IS NOT NULL AND establishment.uf <> '') AS state_count
            FROM cnpj.municipio AS municipality
            LEFT JOIN cnpj.estabelecimento AS establishment
                ON establishment.codigo_municipio = municipality.codigo
            GROUP BY municipality.codigo, municipality.nome
            ORDER BY municipality.codigo
            """
        )
        rows = cursor.fetchall()

    return [
        CnpjMunicipalityContext(
            cnpj_municipality_code=row[0],
            cnpj_municipality_name=row[1],
            inferred_state_abbrev=row[2],
            state_count=int(row[3] or 0),
        )
        for row in rows
    ]


def load_br_municipality_lookup(connection) -> dict[tuple[str, str], list[tuple[str, str]]]:
    lookup: dict[tuple[str, str], list[tuple[str, str]]] = {}
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT municipality_ibge_code, municipality_name, state_abbrev
            FROM geo.br_municipality
            WHERE geobr_year = %s AND is_simplified = %s
            """,
            (GEOBR_YEAR, GEOBR_SIMPLIFIED),
        )
        for municipality_ibge_code, municipality_name, state_abbrev in cursor.fetchall():
            key = (str(state_abbrev).strip().upper(), normalize_name(municipality_name))
            lookup.setdefault(key, []).append((municipality_ibge_code, municipality_name))
    return lookup


def record_issue(cursor, context: CnpjMunicipalityContext, issue_type: str, issue_details: str, candidate_count: int) -> None:
    cursor.execute(
        """
        INSERT INTO geo.cnpj_br_municipality_map_issue (
            cnpj_municipality_code,
            cnpj_municipality_name,
            inferred_state_abbrev,
            candidate_count,
            issue_type,
            issue_details
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            context.cnpj_municipality_code,
            context.cnpj_municipality_name,
            context.inferred_state_abbrev,
            candidate_count,
            issue_type,
            issue_details,
        ),
    )


def rebuild_cnpj_mapping(connection) -> None:
    contexts = load_cnpj_municipality_contexts(connection)
    lookup = load_br_municipality_lookup(connection)

    inserted = 0
    issues = 0
    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE TABLE geo.cnpj_br_municipality_map_issue RESTART IDENTITY")
        cursor.execute("DELETE FROM geo.cnpj_br_municipality_map")

        for context in contexts:
            normalized_name = normalize_name(context.cnpj_municipality_name)
            state_abbrev = (context.inferred_state_abbrev or "").strip().upper()

            if not state_abbrev:
                issue_type = "missing_state" if context.state_count == 0 else "ambiguous_state"
                detail = "No unique UF could be inferred from cnpj.estabelecimento for this municipality code."
                record_issue(cursor, context, issue_type, detail, context.state_count)
                issues += 1
                continue

            candidates = lookup.get((state_abbrev, normalized_name), [])
            if len(candidates) == 1:
                municipality_ibge_code, br_municipality_name = candidates[0]
                cursor.execute(
                    """
                    INSERT INTO geo.cnpj_br_municipality_map (
                        cnpj_municipality_code,
                        municipality_ibge_code,
                        state_abbrev,
                        cnpj_municipality_name,
                        br_municipality_name,
                        match_method,
                        match_score,
                        mapping_source
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        context.cnpj_municipality_code,
                        municipality_ibge_code,
                        state_abbrev,
                        context.cnpj_municipality_name,
                        br_municipality_name,
                        "normalized_name_and_state",
                        1.0,
                        MAPPING_SOURCE,
                    ),
                )
                inserted += 1
                continue

            if not candidates:
                detail = f"No canonical municipality matched normalized name '{normalized_name}' in state '{state_abbrev}'."
                record_issue(cursor, context, "no_match", detail, 0)
                issues += 1
                continue

            detail = f"Multiple canonical municipalities matched normalized name '{normalized_name}' in state '{state_abbrev}'."
            record_issue(cursor, context, "multiple_matches", detail, len(candidates))
            issues += 1

    log(f"Rebuilt CNPJ municipality mapping. mapped={inserted}, issues={issues}")


def validate_geo_load(connection) -> None:
    state_count = fetch_scalar(connection, "SELECT COUNT(*) FROM geo.br_state")
    municipality_count = fetch_scalar(connection, "SELECT COUNT(*) FROM geo.br_municipality")
    mapped_count = fetch_scalar(connection, "SELECT COUNT(*) FROM geo.cnpj_br_municipality_map")
    issue_count = fetch_scalar(connection, "SELECT COUNT(*) FROM geo.cnpj_br_municipality_map_issue")

    if state_count < 27:
        raise RuntimeError(f"Geo bootstrap validation failed: expected at least 27 states, found {state_count}.")
    if municipality_count < 5500:
        raise RuntimeError(
            f"Geo bootstrap validation failed: expected at least 5500 municipalities, found {municipality_count}."
        )

    log(
        "Geo reference ready. "
        f"states={state_count}, municipalities={municipality_count}, mapped_cnpj={mapped_count}, mapping_issues={issue_count}"
    )


def main() -> int:
    connection = connect_postgres()
    try:
        upsert_states(connection)
        upsert_municipalities(connection)
        rebuild_cnpj_mapping(connection)
        validate_geo_load(connection)
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())