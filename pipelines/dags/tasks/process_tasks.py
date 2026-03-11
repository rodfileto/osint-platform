"""
CNPJ Process Tasks — re-export shim

Preserved for backward compatibility. All logic now lives in:
  - tasks.transform_groups  (extract → transform pipeline)
  - tasks.load_groups       (Parquet → PostgreSQL / Neo4j)
"""

# Transform groups & per-file tasks
from .transform_groups import (  # noqa: F401
    REFERENCE_FILES,
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

# Load groups & helpers
from .load_groups import (  # noqa: F401
    AUX_ENTITIES,
    MAIN_ENTITIES_ORDERED,
    ENTITY_TABLE_MAP,
    get_all_processed_months,
    get_parquets_for_month_and_type,
    load_reference_tables_group,
    load_postgres_group,
    load_neo4j_group,
)
