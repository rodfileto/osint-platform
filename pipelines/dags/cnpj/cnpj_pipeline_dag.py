"""
CNPJ pipeline: Receita Federal release → MinIO → Parquet → PostgreSQL → search views → Neo4j.

Runs monthly. With the default params it downloads the latest release (if MinIO is missing any
file), then processes the newest month in the manifest unless it is already fully loaded.

Manual runs:
    airflow dags trigger cnpj_pipeline --conf '{"reference_month": "2026-10"}'
    airflow dags trigger cnpj_pipeline --conf '{"reference_month": "2026-10", "force_reprocess": true}'
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from tasks import download, load_neo4j, load_postgres, transform
from tasks.neo4j_schema import ensure_neo4j_indexes

MAIN_TABLES = ("empresa", "simples_nacional", "estabelecimento", "socio")

with DAG(
    dag_id="cnpj_pipeline",
    description="Download, transform and load the CNPJ registry",
    schedule="0 2 2 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "osint-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "reference_month": Param("auto", type="string", description="YYYY-MM, or 'auto' for the latest release"),
        "force_reprocess": Param(False, type="boolean", description="Transform and load the month again"),
        "load_neo4j": Param(True, type="boolean", description="Also load companies and partners into Neo4j"),
    },
    tags=["cnpj", "etl"],
) as dag:

    @task(execution_timeout=timedelta(hours=4))
    def download_release(params=None):
        return download.download_release(params["reference_month"])

    @task
    def update_manifest():
        download.update_manifest()

    @task.short_circuit
    def select_month(params=None):
        return download.select_month(params["reference_month"], params["force_reprocess"])

    @task
    def list_files(reference_month: str):
        return transform.files_for_month(reference_month)

    @task(max_active_tis_per_dagrun=4, execution_timeout=timedelta(hours=2))
    def transform_file(file_name: str, reference_month: str, params=None):
        return transform.transform_file(reference_month, file_name, params["force_reprocess"])

    @task(execution_timeout=None)
    def load_reference_tables(reference_month: str):
        for table in load_postgres.REFERENCE_TABLES:
            load_postgres.load_table(table, reference_month)

    @task
    def rebuild_geo_mapping():
        import sys

        sys.path.insert(0, "/opt/airflow/scripts/geo")
        import load_br_geo_reference as geo

        conn = geo.connect_postgres()
        try:
            geo.rebuild_cnpj_mapping(conn)
        finally:
            conn.close()

    def load_main_table(table: str):
        @task(task_id=f"load_{table}", execution_timeout=None)
        def _load(reference_month: str):
            return load_postgres.load_table(table, reference_month)
        return _load

    @task(execution_timeout=None)
    def refresh_search_views():
        load_postgres.refresh_search_views()

    @task.short_circuit
    def neo4j_enabled(params=None):
        return params["load_neo4j"]

    @task(execution_timeout=timedelta(hours=12))
    def load_neo4j_entity(entity_type: str, reference_month: str):
        return load_neo4j.load_entity(entity_type, reference_month)

    month = select_month()
    download_release() >> update_manifest() >> month

    transformed = transform_file.partial(reference_month=month).expand(file_name=list_files(month))
    previous = load_reference_tables(month)
    transformed >> previous
    # The mapping infers each municipality's state from its establishments.
    geo_mapping = rebuild_geo_mapping()
    for table in MAIN_TABLES:
        current = load_main_table(table)(month)
        previous >> current
        previous = current
        if table == "estabelecimento":
            current >> geo_mapping

    graph_companies = load_neo4j_entity.override(task_id="load_neo4j_empresas")("Empresa", month)
    graph_partners = load_neo4j_entity.override(task_id="load_neo4j_socios")("Socio", month)
    search_views = refresh_search_views()
    [previous, geo_mapping] >> search_views
    search_views >> neo4j_enabled() >> ensure_neo4j_indexes() >> graph_companies >> graph_partners
