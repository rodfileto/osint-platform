"""
CNPJ Load Groups

Task groups for loading Parquet files into PostgreSQL and Neo4j.
Includes FK-ordered loading helpers and entity table mappings.
"""

import logging
from airflow.decorators import task, task_group

from .load_tasks import load_to_postgresql, load_to_neo4j
from .config import PROCESSED_PATH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Entity loading order — respects FK dependency graph
# ---------------------------------------------------------------------------
# 1. Auxiliary / reference tables (no FK dependencies on other cnpj tables)
AUX_ENTITIES = [
    'cnaes',          # → cnpj.cnae
    'motivos',        # → cnpj.motivo_situacao_cadastral
    'municipios',     # → cnpj.municipio
    'naturezas',      # → cnpj.natureza_juridica
    'paises',         # → cnpj.pais
    'qualificacoes',  # → cnpj.qualificacao_socio
]
# 2. Main transactional tables (loaded after aux; respects intra-group FK order)
#    empresa must precede estabelecimento, simples, and socio
MAIN_ENTITIES_ORDERED = [
    'empresas',           # PK referenced by all below
    'simples',            # FK → empresa
    'estabelecimentos',   # FK → empresa + aux tables
    'socios',             # FK → empresa + aux tables
]

ENTITY_TABLE_MAP = {
    'empresas':          'empresa',
    'estabelecimentos':  'estabelecimento',
    'socios':            'socio',
    'simples':           'simples_nacional',
    'cnaes':             'cnae',
    'motivos':           'motivo_situacao_cadastral',
    'municipios':        'municipio',
    'naturezas':         'natureza_juridica',
    'paises':            'pais',
    'qualificacoes':     'qualificacao_socio',
}


# ============================================================================
# HELPERS
# ============================================================================

@task
def get_all_processed_months() -> list[str]:
    """Scan processed directory for all available months."""
    if not PROCESSED_PATH.exists():
        logger.warning(f"Processed directory not found: {PROCESSED_PATH}")
        return []

    months = []
    for month_dir in sorted(PROCESSED_PATH.iterdir()):
        if month_dir.is_dir() and month_dir.name.count('-') == 1:  # YYYY-MM format
            empresas_files = list(month_dir.glob("empresas_*.parquet"))
            estabelecimentos_files = list(month_dir.glob("estabelecimentos_*.parquet"))
            if empresas_files or estabelecimentos_files:
                months.append(month_dir.name)
                logger.info(
                    f"  Found month: {month_dir.name} "
                    f"({len(empresas_files)} empresas, {len(estabelecimentos_files)} estabelecimentos)"
                )

    logger.info(f"Total processed months found: {len(months)}")
    return months


@task
def get_parquets_for_month_and_type(reference_month: str, entity_type: str) -> list[str]:
    """
    Get all parquet files for a specific month and entity type.

    Args:
        reference_month: Month in YYYY-MM format
        entity_type: 'empresas', 'estabelecimentos', 'socios', 'simples', or aux table name

    Returns:
        List of parquet file paths
    """
    processed_dir = PROCESSED_PATH / reference_month

    if not processed_dir.exists():
        logger.warning(f"Directory not found: {processed_dir}")
        return []

    name_map = {
        'empresas':        'empresas_*.parquet',
        'estabelecimentos':'estabelecimentos_*.parquet',
        'socios':          'socios_*.parquet',
        'simples':         'simples.parquet',
        'cnaes':           'cnaes.parquet',
        'motivos':         'motivos.parquet',
        'municipios':      'municipios.parquet',
        'naturezas':       'naturezas.parquet',
        'paises':          'paises.parquet',
        'qualificacoes':   'qualificacoes.parquet',
    }

    pattern = name_map.get(entity_type.lower())
    if not pattern:
        logger.error(f"Unknown entity type: {entity_type}")
        return []

    parquet_files = [
        str(pq) for pq in processed_dir.glob(pattern)
        if pq.exists() and pq.stat().st_size > 0
    ]

    logger.info(f"Found {len(parquet_files)} {entity_type} parquet files for {reference_month}")
    return sorted(parquet_files)


# ============================================================================
# LOAD TASK GROUPS
# ============================================================================

@task_group
def load_reference_tables_group():
    """
    Load auxiliary / reference tables (cnaes, motivos, municipios, naturezas,
    paises, qualificacoes) for the given reference_month.

    These tables carry no FK dependencies on other cnpj tables and MUST be
    populated before the main transactional tables (empresa, estabelecimento,
    socio) to satisfy FK constraints.

    Reference tables are static per release: only the target month is loaded
    (not 'all'). A TRUNCATE + INSERT strategy is used upstream.
    """
    @task
    def get_params(**context) -> dict:
        from .config import DEFAULT_REFERENCE_MONTH
        return {
            'reference_month': context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH),
        }

    @task(execution_timeout=None)
    def load_aux_tables(task_params: dict, **context) -> dict:
        ref_month = task_params['reference_month']
        if ref_month.lower() == 'all':
            from .config import DEFAULT_REFERENCE_MONTH
            ref_month = DEFAULT_REFERENCE_MONTH
            logger.info(
                f"reference_month='all' is not applicable for reference tables; "
                f"using DEFAULT_REFERENCE_MONTH={ref_month}"
            )

        total_loaded = 0
        for entity in AUX_ENTITIES:
            parquet_files = get_parquets_for_month_and_type.function(ref_month, entity)
            if not parquet_files:
                logger.warning(f"  No parquet found for aux entity '{entity}' / {ref_month} — skipping")
                continue
            table_name = ENTITY_TABLE_MAP[entity]
            logger.info(f"  Loading aux table '{table_name}' from {len(parquet_files)} file(s)…")
            result = load_to_postgresql.function(
                parquet_files=parquet_files,
                table_name=table_name,
                schema="cnpj",
                reference_month=ref_month,
                **context,
            )
            total_loaded += result.get('row_count', 0)
            logger.info(f"  ✓ {table_name}: {result.get('row_count', 0):,} rows loaded")

        logger.info(f"Reference tables done — total rows: {total_loaded:,}")
        return {"total_loaded": total_loaded}

    params = get_params()
    load_aux_tables(params)  # type: ignore[arg-type]


@task_group
def load_postgres_group():
    """
    Load main transactional CNPJ tables to PostgreSQL.

    Loads in FK-safe order: empresa → simples → estabelecimento → socio.
    Assumes reference tables are already populated (run load_reference_tables_group first).
    Supports specific month or 'all', and specific entity or 'all'.
    """
    @task
    def get_params(**context) -> dict:
        from .config import DEFAULT_REFERENCE_MONTH
        return {
            'reference_month': context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH),
            'entity_type': context['params'].get('entity_type', 'all'),
        }

    @task(execution_timeout=None)  # UPSERT de 135M rows pode levar várias horas
    def load_to_pg(task_params: dict, **context) -> dict:
        ref_month = task_params['reference_month']
        entity_type = task_params['entity_type']
        max_files = int(context.get('params', {}).get('max_files', 0))

        months_to_process = [ref_month]
        if ref_month.lower() == 'all':
            months_to_process = get_all_processed_months.function()

        if entity_type.lower() in MAIN_ENTITIES_ORDERED:
            entities_to_process = [entity_type.lower()]
        elif entity_type.lower() in AUX_ENTITIES:
            logger.warning(
                f"entity_type='{entity_type}' is a reference table — "
                "use load_reference_tables_group instead. Skipping."
            )
            return {"total_loaded": 0}
        else:
            entities_to_process = MAIN_ENTITIES_ORDERED  # FK-safe order

        total_loaded = 0

        for month in months_to_process:
            logger.info(f"Loading main tables to Postgres for month: {month}")

            for entity in entities_to_process:
                parquet_files = get_parquets_for_month_and_type.function(month, entity)
                if not parquet_files:
                    logger.info(f"  No parquet files found for {entity} / {month} — skipping")
                    continue
                if max_files > 0:
                    logger.info(f"  max_files={max_files}: limiting to {max_files} of {len(parquet_files)} files")
                    parquet_files = parquet_files[:max_files]
                table_name = ENTITY_TABLE_MAP[entity]
                result = load_to_postgresql.function(
                    parquet_files=parquet_files,
                    table_name=table_name,
                    schema="cnpj",
                    reference_month=month,
                    **context,
                )
                total_loaded += result.get('row_count', 0)
                logger.info(f"  ✓ {table_name}: {result.get('row_count', 0):,} rows loaded")

        return {"total_loaded": total_loaded}

    params = get_params()
    load_to_pg(params)  # type: ignore[arg-type]


@task_group
def load_neo4j_group():
    """
    Load existing parquet files to Neo4j.
    Supports specific month or 'all', and specific entity or 'all'.
    """
    @task
    def get_params(**context) -> dict:
        from .config import DEFAULT_REFERENCE_MONTH
        return {
            'reference_month': context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH),
            'entity_type': context['params'].get('entity_type', 'all'),
        }

    @task
    def load_to_neo(task_params: dict, **context) -> dict:
        ref_month = task_params['reference_month']
        entity_type = task_params['entity_type']

        months_to_process = [ref_month]
        if ref_month.lower() == 'all':
            months_to_process = get_all_processed_months.function()

        # Cenário A: Estabelecimento removido do Neo4j.
        # Filiais são consultadas via JOIN com cnpj.mv_company_search no PostgreSQL.
        # O grafo Neo4j fica focado em estrutura societária: Empresa ← SOCIO_DE → Pessoa.
        NEO4J_ENTITIES = ['empresas', 'socios']

        if entity_type.lower() == 'estabelecimentos':
            logger.warning(
                "entity_type='estabelecimentos' ignorado: Estabelecimento foi removido do Neo4j "
                "(Cenário A). Filiais consultadas via mv_company_search no PostgreSQL."
            )
            return {"total_nodes": 0, "total_relationships": 0, "skipped": "estabelecimentos_not_in_neo4j"}

        if entity_type.lower() in NEO4J_ENTITIES:
            entities_to_process = [entity_type.lower()]
        else:
            entities_to_process = NEO4J_ENTITIES

        total_nodes = 0
        total_rels = 0

        for month in months_to_process:
            logger.info(f"Loading to Neo4j for month: {month}")

            if 'empresas' in entities_to_process:
                empresas_files = get_parquets_for_month_and_type.function(month, 'empresas')
                if empresas_files:
                    result = load_to_neo4j.function(
                        parquet_files=empresas_files,
                        entity_type="Empresa",
                        **context,
                    )
                    total_nodes += result.get('total_nodes', 0)

            if 'socios' in entities_to_process:
                socios_files = get_parquets_for_month_and_type.function(month, 'socios')
                if socios_files:
                    result = load_to_neo4j.function(
                        parquet_files=socios_files,
                        entity_type="Socio",
                        **context,
                    )
                    total_nodes += result.get('total_nodes', 0)
                    total_rels += result.get('total_relationships', 0)

        return {"total_nodes": total_nodes, "total_relationships": total_rels}

    params = get_params()
    load_to_neo(params)  # type: ignore[arg-type]
