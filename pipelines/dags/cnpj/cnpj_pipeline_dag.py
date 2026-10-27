"""
CNPJ pipeline: Receita Federal release → MinIO → Parquet → gold → PostgreSQL.

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
from airflow.operators.bash import BashOperator

from tasks import download, transform

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

    month = select_month()
    download_release() >> update_manifest() >> month

    transformed = transform_file.partial(reference_month=month).expand(file_name=list_files(month))

    # The selected month reaches the gold commands as an environment variable (env is a
    # templated field; append_env keeps the container's MinIO and PostgreSQL settings).
    GOLD_ENV = {"REFERENCE_MONTH": "{{ ti.xcom_pull(task_ids='select_month') }}"}

    # dbt-duckdb builds the gold Parquet (latest-month tables + socio_history) in MinIO
    # from the silver layer. The guard skips the build for months older than the latest silver.
    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            'python /opt/airflow/scripts/cnpj/gold_guard.py "$REFERENCE_MONTH" '
            "&& mkdir -p /opt/airflow/temp_ssd/dbt "
            "&& dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            '--select cnpj_gold --vars "{\\"reference_month\\": \\"$REFERENCE_MONTH\\"}"'
        ),
        env=GOLD_ENV,
        append_env=True,
        skip_exit_code=2,
        execution_timeout=timedelta(hours=4),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    transformed >> build_gold

    # The publish copies the gold Parquet into *_next tables and swaps them in atomically.
    publish_gold = BashOperator(
        task_id="publish_gold",
        bash_command='python /opt/airflow/scripts/cnpj/gold_publish.py "$REFERENCE_MONTH"',
        env=GOLD_ENV,
        append_env=True,
        skip_exit_code=2,
        execution_timeout=timedelta(hours=4),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    build_gold >> publish_gold

    # The mapping infers each municipality's state from its establishments.
    geo_mapping = rebuild_geo_mapping()
    publish_gold >> geo_mapping
