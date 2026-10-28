"""
OpenAlex pipeline: public Parquet snapshot → bronze (MinIO) → silver (NVMe) → DuckDB catalog.

OpenAlex publishes a full release quarterly, on the second Wednesday of January, April, July and
October (UTC), replacing its bucket in place. Cron cannot say "second Wednesday" (croniter ORs
day-of-month and weekday), so the DAG runs daily on the 8th-21st of those months; once a release
is synced, verified and built, each run stops at check_release. A run that meets the bucket while
it is being replaced fails in sync and the next day's run retries it.

Manual runs:
    airflow dags trigger openalex_pipeline
    airflow dags trigger openalex_pipeline --conf '{"force_silver": true, "tables": ["work_references"]}'
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

from openalex import catalog, silver, state, storage, sync  # noqa: E402
from openalex.config import Lake  # noqa: E402

with DAG(
    dag_id="openalex_pipeline",
    description="Mirror the OpenAlex snapshot into bronze and build silver and the catalog",
    schedule="0 6 8-21 1,4,7,10 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "osint-platform", "retries": 1, "retry_delay": timedelta(minutes=30)},
    params={
        "force_silver": Param(False, type="boolean", description="Rebuild every silver file"),
        "tables": Param([], type="array", description="Silver tables to build (default: all)"),
        "dry_run": Param(False, type="boolean", description="Only log what sync would download"),
    },
    tags=["openalex", "lake"],
) as dag:

    @task.short_circuit
    def check_release(params=None):
        """Continue when OpenAlex has a newer release, or the current one is not fully built."""
        if params["force_silver"] or params["tables"] or params["dry_run"]:
            return True
        upstream = sync.upstream_release(Lake.from_env())
        conn = state.connect()
        try:
            latest = state.latest_release(conn)
        finally:
            conn.close()
        done = (
            latest is not None
            and latest["release_date"].isoformat() == upstream
            and all(latest[mark] for mark in state.RELEASE_MARKS)
        )
        print(f"upstream release {upstream}; ours: {latest}")
        return not done

    @task.short_circuit(execution_timeout=None)
    def sync_bronze(params=None):
        """Mirror the release; a dry run logs the plan and skips the rest of the run."""
        if not sync.sync(Lake.from_env(), dry_run=params["dry_run"]):
            raise AirflowFailException("sync incomplete; see the log (the next scheduled run retries)")
        return not params["dry_run"]

    @task(execution_timeout=timedelta(hours=2))
    def verify_bronze():
        if not catalog.verify(Lake.from_env()):
            raise AirflowFailException("bronze does not match the manifest")

    @task
    def silver_tables(params=None):
        return params["tables"] or list(silver.TABLES)

    @task(execution_timeout=None, max_active_tis_per_dagrun=2)
    def build_silver(table: str, params=None):
        silver.build_table(Lake.from_env(), table, force=params["force_silver"])

    @task
    def build_catalog(params=None):
        lake = Lake.from_env()
        if not params["tables"]:
            # Every silver table was brought up to date in this run.
            conn = state.connect()
            try:
                release = catalog.bronze_manifest(lake, storage.s3_client())["date"]
                state.mark_release(conn, release, "silver_built_at")
            finally:
                conn.close()
        catalog.build_catalog(lake)

    built = build_silver.expand(table=silver_tables())
    check_release() >> sync_bronze() >> verify_bronze() >> built >> build_catalog()
