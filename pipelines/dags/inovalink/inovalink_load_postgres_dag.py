"""Inovalink PostgreSQL load DAG."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.utils.trigger_rule import TriggerRule  # type: ignore

sys.path.insert(
    0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts")
)

from inovalink.loader import (  # noqa: E402
    collect_livewire_payload,
    load_organization_relationship,
    load_organization_snapshot,
    mark_manifest_failed,
    mark_manifest_loaded,
    register_sync_manifest,
    resolve_exact_cnpj_matches,
    seed_organization_cnpj_match,
    store_component_payload,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _collect_livewire_payload(**context) -> dict:
    return collect_livewire_payload()


def _register_sync_manifest(**context) -> dict:
    ti = context["task_instance"]
    payload = ti.xcom_pull(task_ids="collect_livewire_payload", key="return_value")
    if not isinstance(payload, dict):
        raise RuntimeError("Collector did not return a valid payload.")
    return register_sync_manifest(payload)


def _store_component_payload(**context) -> dict:
    ti = context["task_instance"]
    payload = ti.xcom_pull(task_ids="collect_livewire_payload", key="return_value")
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest)
    return store_component_payload(manifest_id, payload)


def _load_organization_snapshot(**context) -> dict:
    ti = context["task_instance"]
    payload = ti.xcom_pull(task_ids="collect_livewire_payload", key="return_value")
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest)
    actors = payload.get("actors") if isinstance(payload, dict) else None
    if not isinstance(actors, dict):
        raise RuntimeError("Collector payload did not include decoded actors.")
    return load_organization_snapshot(manifest_id, actors)


def _load_organization_relationship(**context) -> dict:
    ti = context["task_instance"]
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest)
    return load_organization_relationship(manifest_id)


def _seed_organization_cnpj_match(**context) -> dict:
    ti = context["task_instance"]
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest)
    return seed_organization_cnpj_match(manifest_id)


def _resolve_exact_cnpj_matches(**context) -> dict:
    ti = context["task_instance"]
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest)
    return resolve_exact_cnpj_matches(manifest_id)


def _mark_manifest_loaded(**context) -> dict:
    ti = context["task_instance"]
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest)
    return mark_manifest_loaded(manifest_id)


def _mark_manifest_failed(**context) -> None:
    ti = context["task_instance"]
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value")
    manifest_id = _require_manifest_id(manifest, allow_missing=True)
    if manifest_id is None:
        logger.warning("No manifest id available to mark as failed.")
        return

    failed_tasks = []
    for task_id in (
        "store_component_payload",
        "load_organization_snapshot",
        "load_organization_relationship",
        "seed_organization_cnpj_match",
        "resolve_exact_cnpj_matches",
    ):
        task_instance = ti.get_dagrun().get_task_instance(task_id)
        if task_instance and task_instance.state == "failed":
            failed_tasks.append(task_id)

    error_message = "Failed tasks: " + ", ".join(failed_tasks) if failed_tasks else "Inovalink pipeline failed."
    mark_manifest_failed(manifest_id, error_message)


def _generate_report(**context) -> None:
    ti = context["task_instance"]
    manifest = ti.xcom_pull(task_ids="register_sync_manifest", key="return_value") or {}
    snapshot = ti.xcom_pull(task_ids="load_organization_snapshot", key="return_value") or {}
    relationships = ti.xcom_pull(task_ids="load_organization_relationship", key="return_value") or {}
    cnpj_seed = ti.xcom_pull(task_ids="seed_organization_cnpj_match", key="return_value") or {}
    cnpj_exact = ti.xcom_pull(task_ids="resolve_exact_cnpj_matches", key="return_value") or {}
    loaded = ti.xcom_pull(task_ids="mark_manifest_loaded", key="return_value") or {}

    lines = [
        "=" * 65,
        "  Inovalink — PostgreSQL Load Report",
        f"  Run ID : {context['run_id']}",
        f"  Data   : {datetime.utcnow().isoformat(timespec='seconds')} UTC",
        "=" * 65,
        f"  Manifest id                      : {manifest.get('manifest_id')}",
        f"  Snapshot rows                    : {snapshot.get('rows_inserted', 0):>10}",
        f"  Relationship rows                : {relationships.get('rows_inserted', 0):>10}",
        f"  Relationship unresolved          : {relationships.get('rows_unresolved', 0):>10}",
        f"  CNPJ seed rows                   : {cnpj_seed.get('rows_seeded', 0):>10}",
        f"  CNPJ rows with source_cnpj       : {cnpj_seed.get('rows_with_source_cnpj', 0):>10}",
        f"  CNPJ exact matches               : {cnpj_exact.get('matched_rows', 0):>10}",
        f"  CNPJ exact unmatched             : {cnpj_exact.get('unmatched_rows', 0):>10}",
        f"  Final manifest status            : {loaded.get('processing_status', 'not_loaded')}",
        "=" * 65,
    ]
    report = "\n".join(lines)
    logger.info("\n%s", report)
    print(report)


def _require_manifest_id(manifest: dict | None, allow_missing: bool = False) -> int | None:
    if isinstance(manifest, dict) and manifest.get("manifest_id") is not None:
        return int(manifest["manifest_id"])
    if allow_missing:
        return None
    raise RuntimeError("Manifest task did not return a manifest_id.")


with DAG(
    dag_id="inovalink_load_postgres",
    default_args=default_args,
    description="Collects the public Inovalink Livewire payload and loads it into PostgreSQL.",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["inovalink", "load", "postgres"],
) as dag:
    collect_task = PythonOperator(
        task_id="collect_livewire_payload",
        python_callable=_collect_livewire_payload,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    register_task = PythonOperator(
        task_id="register_sync_manifest",
        python_callable=_register_sync_manifest,
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
    )

    component_task = PythonOperator(
        task_id="store_component_payload",
        python_callable=_store_component_payload,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    snapshot_task = PythonOperator(
        task_id="load_organization_snapshot",
        python_callable=_load_organization_snapshot,
        provide_context=True,
        execution_timeout=timedelta(minutes=20),
    )

    relationship_task = PythonOperator(
        task_id="load_organization_relationship",
        python_callable=_load_organization_relationship,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    cnpj_seed_task = PythonOperator(
        task_id="seed_organization_cnpj_match",
        python_callable=_seed_organization_cnpj_match,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    cnpj_exact_task = PythonOperator(
        task_id="resolve_exact_cnpj_matches",
        python_callable=_resolve_exact_cnpj_matches,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )

    mark_loaded_task = PythonOperator(
        task_id="mark_manifest_loaded",
        python_callable=_mark_manifest_loaded,
        provide_context=True,
        execution_timeout=timedelta(minutes=5),
    )

    mark_failed_task = PythonOperator(
        task_id="mark_manifest_failed",
        python_callable=_mark_manifest_failed,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
        execution_timeout=timedelta(minutes=5),
    )

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    collect_task >> register_task >> component_task >> snapshot_task  # type: ignore
    snapshot_task >> relationship_task >> mark_loaded_task  # type: ignore
    snapshot_task >> cnpj_seed_task >> cnpj_exact_task >> mark_loaded_task  # type: ignore
    [component_task, snapshot_task, relationship_task, cnpj_seed_task, cnpj_exact_task] >> mark_failed_task  # type: ignore
    [mark_loaded_task, mark_failed_task] >> report_task  # type: ignore