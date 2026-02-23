"""
CNPJ Ingestion Task Modules

Modular task definitions for the CNPJ data pipeline.
"""

from .extract_tasks import extract_zip_file
from .transform_tasks import transform_empresas_duckdb, transform_estabelecimentos_duckdb
from .load_tasks import load_to_postgresql, load_to_neo4j
from .process_tasks import (
    process_empresas_file,
    process_estabelecimentos_file,
    transform_empresas_group,
    transform_estabelecimentos_group,
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
    'transform_empresas_group',
    'transform_estabelecimentos_group',
    'load_postgres_group',
    'load_neo4j_group',
]
