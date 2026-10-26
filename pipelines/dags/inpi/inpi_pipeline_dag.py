"""
INPI pipeline: download the 10 patent CSVs, stage each table, publish them together and refresh
the search view.

A CSV is stored as a new MinIO snapshot only when its content changed. The tables are replaced
in one transaction (publish), so the API never sees a partial load. The run stops after the
downloads when every dataset's newest snapshot is already loaded.

Manual runs:
    airflow dags trigger inpi_pipeline --conf '{"force_reload": true}'
    airflow dags trigger inpi_pipeline --conf '{"force_download": true}'
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

from inpi import loader  # noqa: E402
from inpi.downloader import INPI_DATASETS, collect_dataset  # noqa: E402

with DAG(
    dag_id="inpi_pipeline",
    description="Download INPI patent data and load PostgreSQL",
    schedule="0 5 * * 1",
    start_date=datetime(2026, 1, 5),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "osint-platform", "retries": 2, "retry_delay": timedelta(minutes=10)},
    params={
        "force_download": Param(False, type="boolean", description="Download even if INPI reports no change"),
        "force_reload": Param(False, type="boolean", description="Reload the tables even if nothing new arrived"),
    },
    tags=["inpi", "etl"],
) as dag:

    @task(execution_timeout=timedelta(hours=1))
    def download(dataset_name: str, params=None):
        return collect_dataset(dataset_name, INPI_DATASETS[dataset_name], force=params["force_download"])

    @task.short_circuit
    def needs_load(params=None):
        return params["force_reload"] or loader.has_pending_snapshot()

    @task(max_active_tis_per_dagrun=3, execution_timeout=timedelta(hours=1))
    def stage(table: str):
        return loader.stage_table(table)

    @task(execution_timeout=timedelta(hours=2))
    def publish():
        return loader.publish()

    @task(execution_timeout=timedelta(hours=1))
    def refresh_search_view():
        loader.refresh_search_view()

    gate = needs_load()
    for name in INPI_DATASETS:
        download.override(task_id=f"download_{name}")(name) >> gate

    published = publish()
    for table in loader.TABLES:
        gate >> stage.override(task_id=f"stage_{table}")(table) >> published

    published >> refresh_search_view()
