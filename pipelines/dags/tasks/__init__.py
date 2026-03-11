"""
CNPJ Ingestion Task Modules

Modular task definitions for the CNPJ data pipeline.
"""

from .extract_tasks import extract_zip_file
from .transform_tasks import transform_empresas_duckdb, transform_estabelecimentos_duckdb
from .load_tasks import load_to_postgresql, load_to_neo4j
from .transform_groups import (
    process_empresas_file,
    process_estabelecimentos_file,
    process_socios_file,
    process_simples_file,
    process_reference_file,
    transform_empresas_group,
    transform_estabelecimentos_group,
    transform_socios_group,
    transform_simples_group,
    transform_references_group,
)
from .load_groups import (
    get_all_processed_months,
    get_parquets_for_month_and_type,
    load_reference_tables_group,
    load_postgres_group,
    load_neo4j_group,
)

__all__ = [
    'extract_zip_file',
    'transform_empresas_duckdb',
    'transform_estabelecimentos_duckdb',
    'load_to_postgresql',
    'load_to_neo4j',
    'process_empresas_file',
    'process_estabelecimentos_file',
    'process_socios_file',
    'process_simples_file',
    'process_reference_file',
    'transform_empresas_group',
    'transform_estabelecimentos_group',
    'transform_socios_group',
    'transform_simples_group',
    'transform_references_group',
    'get_all_processed_months',
    'get_parquets_for_month_and_type',
    'load_reference_tables_group',
    'load_postgres_group',
    'load_neo4j_group',
]
