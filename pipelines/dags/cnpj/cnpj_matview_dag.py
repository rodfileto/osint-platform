"""
CNPJ Materialized View DAG

Popula ou atualiza as materialized views cnpj.mv_company_search (ativas) e
cnpj.mv_company_search_inactive (inativas). Estrutura e índices são criados
pelas migrations Flyway (WITH NO DATA); esta DAG apenas gerencia os dados.

Fluxo:
    start
      → ensure_pg_trgm   # garante extensão pg_trgm (idempotente)
      → ensure_mv_exists # REFRESH inicial (sem CONCURRENTLY) ou REFRESH CONCURRENTLY
      → analyze_mv       # ANALYZE para atualizar estatísticas do planner
    end → branch_neo4j

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
    A estrutura da MatView e seus índices são criados pelas migrations Flyway (WITH NO DATA).
    Esta task apenas popula ou atualiza os dados para ambas MVs (ativa e inativa):
    - Se vazia (primeira carga): REFRESH sem CONCURRENTLY (índices ainda sem dados).
    - Se já populada: REFRESH CONCURRENTLY (índice único já populado; não bloqueia leituras).
    """
    conn = _get_pg_conn()
    conn.autocommit = True

    with conn.cursor() as cur:
        # Verifica se as MVs já foram populadas consultando o catálogo do Postgres
        cur.execute("""
            SELECT relname, relispopulated 
            FROM pg_class 
            WHERE relname IN ('mv_company_search', 'mv_company_search_inactive') 
              AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'cnpj');
        """)
        results = cur.fetchall()
        
        is_active_populated = False
        is_inactive_populated = False
        
        for row in results:
            if row[0] == 'mv_company_search':
                is_active_populated = row[1]
            elif row[0] == 'mv_company_search_inactive':
                is_inactive_populated = row[1]

    conn.close()

    conn2 = _get_pg_conn()
    conn2.autocommit = True
    with conn2.cursor() as cur:
        cur.execute("SET max_parallel_workers_per_gather = 0;")
        
        # ACTIVE
        if not is_active_populated:
            logger.info("MatView ativa não populada — executando REFRESH inicial (pode levar 20-40 min)...")
            cur.execute("REFRESH MATERIALIZED VIEW cnpj.mv_company_search;")
            logger.info("REFRESH inicial ativa concluído.")
        else:
            logger.info("MatView ativa populada — executando REFRESH CONCURRENTLY (não bloqueia leituras)...")
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY cnpj.mv_company_search;")
            logger.info("REFRESH CONCURRENTLY ativa concluído.")
            
        # INACTIVE
        if not is_inactive_populated:
            logger.info("MatView inativa não populada — executando REFRESH inicial (pode levar 20-40 min)...")
            cur.execute("REFRESH MATERIALIZED VIEW cnpj.mv_company_search_inactive;")
            logger.info("REFRESH inicial inativa concluído.")
        else:
            logger.info("MatView inativa populada — executando REFRESH CONCURRENTLY (não bloqueia leituras)...")
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY cnpj.mv_company_search_inactive;")
            logger.info("REFRESH CONCURRENTLY inativa concluído.")
    conn2.close()


@task
def analyze_mv():
    """ANALYZE na MatView para que o planner use estatísticas atualizadas."""
    conn = _get_pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("ANALYZE cnpj.mv_company_search;")
        
        cur.execute("""
            SELECT
                pg_size_pretty(pg_total_relation_size('cnpj.mv_company_search')) AS total_size,
                (SELECT COUNT(*) FROM cnpj.mv_company_search) AS row_count
        """)
        row = cur.fetchone()
        if row:
            logger.info(f"MatView ativa pronta — tamanho: {row[0]}, registros: {row[1]:,}")

        cur.execute("ANALYZE cnpj.mv_company_search_inactive;")
        
        cur.execute("""
            SELECT
                pg_size_pretty(pg_total_relation_size('cnpj.mv_company_search_inactive')) AS total_size,
                (SELECT COUNT(*) FROM cnpj.mv_company_search_inactive) AS row_count
        """)
        row_inact = cur.fetchone()
        if row_inact:
            logger.info(f"MatView inativa pronta — tamanho: {row_inact[0]}, registros: {row_inact[1]:,}")
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
    'execution_timeout': None,  # REFRESH de 135M rows pode levar várias horas
}

with DAG(
    dag_id='cnpj_matview_refresh',
    default_args=default_args,
    description='Popula ou atualiza as materialized views cnpj.mv_company_search (ativas/inativas)',
    schedule_interval=None,   # trigger manual ou via TriggerDagRunOperator
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['cnpj', 'postgres', 'matview', 'search'],
    params={
        'reference_month': DEFAULT_REFERENCE_MONTH,
        'trigger_neo4j': True,  # Permite pular a carga do Neo4j se quiser apenas testar o Postgres
    },
) as dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    t_trgm    = ensure_pg_trgm()
    t_mv      = ensure_mv_exists()
    t_analyze = analyze_mv()

    @task.branch
    def check_trigger_neo4j(**context):
        params = context.get('params', {})
        if params.get('trigger_neo4j', True):
            return 'trigger_load_neo4j'
        return 'skip_neo4j_trigger'

    branch_neo4j = check_trigger_neo4j()
    skip_trigger = EmptyOperator(task_id='skip_neo4j_trigger')

    # Encadeia automaticamente com o próximo passo do pipeline (se habilitado)
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

    start >> t_trgm >> t_mv >> t_analyze >> end >> branch_neo4j
    branch_neo4j >> trigger_neo4j
    branch_neo4j >> skip_trigger
