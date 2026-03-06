"""
CNPJ Materialized View DAG

Cria ou atualiza a materialized view cnpj.mv_company_search no tablespace fast_ssd (SSD).

Fluxo:
    start
      → ensure_pg_trgm         # habilita extensão se ausente
      → ensure_mv_exists        # cria MatView se não existir, senão faz REFRESH CONCURRENTLY
      → ensure_indexes          # cria índices no SSD se ausentes
      → analyze_mv              # ANALYZE para atualizar estatísticas do planner
    end

Trigger: manual ou via TriggerDagRunOperator ao fim da cnpj_load_postgres.
"""

import os
import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from tasks.config import DEFAULT_REFERENCE_MONTH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conexão PostgreSQL
# ---------------------------------------------------------------------------

def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password"),
    )

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task
def ensure_pg_trgm():
    """Garante que a extensão pg_trgm está instalada."""
    conn = _get_pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    conn.close()
    logger.info("pg_trgm OK")


@task
def ensure_mv_exists():
    """
    A estrutura da MatView é sempre criada pelo init-cnpj-schema.sql (WITH NO DATA).
    Esta task apenas popula ou atualiza os dados:
    - Se vazia (primeira carga): REFRESH sem CONCURRENTLY.
    - Se já populada: REFRESH CONCURRENTLY (não bloqueia leituras).
    """
    conn = _get_pg_conn()
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM cnpj.mv_company_search LIMIT 1;")
        row_count = cur.fetchone()[0]

    conn.close()

    conn2 = _get_pg_conn()
    conn2.autocommit = True
    with conn2.cursor() as cur:
        cur.execute("SET max_parallel_workers_per_gather = 0;")
        if row_count == 0:
            logger.info("MatView vazia — executando REFRESH inicial (pode levar 20-40 min)...")
            cur.execute("REFRESH MATERIALIZED VIEW cnpj.mv_company_search;")
            logger.info("REFRESH inicial concluído.")
        else:
            logger.info(f"MatView tem {row_count:,} registros — executando REFRESH CONCURRENTLY...")
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY cnpj.mv_company_search;")
            logger.info("REFRESH CONCURRENTLY concluído.")
    conn2.close()


@task
def ensure_indexes():
    """
    Cria os índices no tablespace fast_ssd se ainda não existirem.
    Usa CREATE INDEX IF NOT EXISTS para ser idempotente.
    """
    indexes = [
        (
            "idx_mv_cnpj14",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cnpj14 "
            "ON cnpj.mv_company_search(cnpj_14) TABLESPACE fast_ssd;",
        ),
        (
            "idx_mv_razao_social_trgm",
            "CREATE INDEX IF NOT EXISTS idx_mv_razao_social_trgm "
            "ON cnpj.mv_company_search USING gin (razao_social gin_trgm_ops) TABLESPACE fast_ssd;",
        ),
        (
            "idx_mv_nome_fantasia_trgm",
            "CREATE INDEX IF NOT EXISTS idx_mv_nome_fantasia_trgm "
            "ON cnpj.mv_company_search USING gin (nome_fantasia gin_trgm_ops) TABLESPACE fast_ssd;",
        ),
        (
            "idx_mv_uf_municipio",
            "CREATE INDEX IF NOT EXISTS idx_mv_uf_municipio "
            "ON cnpj.mv_company_search(uf, municipio) TABLESPACE fast_ssd;",
        ),
    ]

    conn = _get_pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        for idx_name, ddl in indexes:
            logger.info(f"Garantindo índice {idx_name}...")
            cur.execute(ddl)
            logger.info(f"  {idx_name} OK")
    conn.close()


@task
def analyze_mv():
    """ANALYZE na MatView para que o planner use estatísticas atualizadas."""
    conn = _get_pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("ANALYZE cnpj.mv_company_search;")
        # Coleta tamanho final para log
        cur.execute("""
            SELECT
                pg_size_pretty(pg_total_relation_size('cnpj.mv_company_search')) AS total_size,
                (SELECT COUNT(*) FROM cnpj.mv_company_search) AS row_count
        """)
        row = cur.fetchone()
        logger.info(f"MatView pronta — tamanho: {row[0]}, registros: {row[1]:,}")
    conn.close()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    'owner': 'osint-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=3),
}

with DAG(
    dag_id='cnpj_matview_refresh',
    default_args=default_args,
    description='Cria ou atualiza a materialized view cnpj.mv_company_search no SSD',
    schedule_interval=None,   # trigger manual ou via TriggerDagRunOperator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['cnpj', 'postgres', 'matview', 'search'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH,
    },
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    t_trgm    = ensure_pg_trgm()
    t_mv      = ensure_mv_exists()
    t_indexes = ensure_indexes()
    t_analyze = analyze_mv()

    # Encadeia automaticamente com o próximo passo do pipeline
    trigger_neo4j = TriggerDagRunOperator(
        task_id='trigger_load_neo4j',
        trigger_dag_id='cnpj_load_neo4j',
        conf={
            'reference_month': '{{ params.reference_month }}',
            'entity_type': 'all',
            'force_reprocess': False,
        },
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        execution_timeout=None,  # neo4j pode levar ~4h
    )

    start >> t_trgm >> t_mv >> t_indexes >> t_analyze >> end >> trigger_neo4j
