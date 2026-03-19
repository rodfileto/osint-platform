"""
FINEP NLP Topic Modelling DAG

Runs the BERTopic pipeline over finep.projetos_operacao_direta
(resumo_publicavel with fallback to titulo) and persists results to:
  - PostgreSQL: finep.nlp_model_run, finep.nlp_topic, finep.nlp_document_topic
  - Neo4j:      ProjetoFinep nodes (topic_id, umap_x/y) + SIMILAR_TO edges

Task flow:
  start
    └─▶ embed_and_cluster    — embed texts, BERTopic, write PG schema
          └─▶ load_neo4j_nodes  — MERGE ProjetoFinep + PROPOSTO_POR / EXECUTADO_POR
                └─▶ load_neo4j_edges — CREATE SIMILAR_TO, clean up .npy artifacts
                      └─▶ end

Trigger:
  - Manual via UI or CLI
  - Can be triggered by finep_load_operacao_direta via TriggerDagRunOperator

All hyperparameters can be overridden at trigger time via conf:
  {
    "embedding_model":           "paraphrase-multilingual-mpnet-base-v2",
    "umap_n_neighbors":          15,
    "umap_n_components_cluster": 5,
    "sim_threshold":             0.70,
    "k_neighbors":               5
  }
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts/finep"))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "osint-platform",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 17),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _embed_and_cluster(**context) -> dict:
    from nlp_topic_pipeline import (
        embed_and_cluster,
        EMBEDDING_MODEL,
        UMAP_N_NEIGHBORS,
        UMAP_N_COMPONENTS_CLUSTER,
        SIM_THRESHOLD,
        K_NEIGHBORS,
    )

    conf = context["dag_run"].conf or {}
    result = embed_and_cluster(
        dag_run_id=context["dag_run"].run_id,
        embedding_model=conf.get("embedding_model", EMBEDDING_MODEL),
        umap_n_neighbors=int(conf.get("umap_n_neighbors", UMAP_N_NEIGHBORS)),
        umap_n_components_cluster=int(conf.get("umap_n_components_cluster", UMAP_N_COMPONENTS_CLUSTER)),
        sim_threshold=float(conf.get("sim_threshold", SIM_THRESHOLD)),
        k_neighbors=int(conf.get("k_neighbors", K_NEIGHBORS)),
    )
    logger.info(
        f"embed_and_cluster done | run_id={result['run_id']} | "
        f"{result['n_documents']} docs | {result['n_topics']} topics | "
        f"{result['n_outliers']} outliers"
    )
    return result


def _load_neo4j_nodes(**context) -> dict:
    from nlp_topic_pipeline import load_neo4j_nodes

    embed_result = context["ti"].xcom_pull(task_ids="embed_and_cluster")
    result = load_neo4j_nodes(run_id=embed_result["run_id"])
    logger.info(
        f"load_neo4j_nodes done | {result['nodes_merged']} merged | "
        f"PROPOSTO_POR={result['proposto_created']} | "
        f"EXECUTADO_POR={result['executado_created']}"
    )
    return result


def _load_neo4j_edges(**context) -> dict:
    from nlp_topic_pipeline import load_neo4j_edges

    embed_result = context["ti"].xcom_pull(task_ids="embed_and_cluster")
    result = load_neo4j_edges(
        run_id=embed_result["run_id"],
        artifact_dir=embed_result["artifact_dir"],
        sim_threshold=embed_result["sim_threshold"],
        k_neighbors=embed_result["k_neighbors"],
    )
    logger.info(f"load_neo4j_edges done | {result['edges_created']} SIMILAR_TO created")
    return result


with DAG(
    dag_id="finep_nlp_topic",
    default_args=default_args,
    description=(
        "BERTopic pipeline sobre projetos FINEP (resumo/título) → "
        "PostgreSQL (topics) + Neo4j (ProjetoFinep + SIMILAR_TO)"
    ),
    schedule_interval=None,   # triggered manually or by finep_load_operacao_direta
    catchup=False,
    max_active_runs=1,
    params={
        "embedding_model":           "paraphrase-multilingual-mpnet-base-v2",
        "umap_n_neighbors":          15,
        "umap_n_components_cluster": 5,
        "sim_threshold":             0.70,
        "k_neighbors":               5,
    },
    tags=["finep", "nlp", "bertopic", "neo4j", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    embed_task = PythonOperator(
        task_id="embed_and_cluster",
        python_callable=_embed_and_cluster,
        provide_context=True,
        execution_timeout=timedelta(hours=2),   # embedding large corpora is slow
    )

    neo4j_nodes_task = PythonOperator(
        task_id="load_neo4j_nodes",
        python_callable=_load_neo4j_nodes,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    neo4j_edges_task = PythonOperator(
        task_id="load_neo4j_edges",
        python_callable=_load_neo4j_edges,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    end = EmptyOperator(task_id="end")

    start >> embed_task >> neo4j_nodes_task >> neo4j_edges_task >> end
