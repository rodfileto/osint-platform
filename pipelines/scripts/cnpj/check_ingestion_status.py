#!/usr/bin/env python3
"""
CNPJ Ingestion Live Status Monitor

Usage (from host):
  docker-compose exec airflow-webserver python /opt/airflow/scripts/cnpj/check_ingestion_status.py

Auto-refresh (every 10s):
  watch -n 10 'docker-compose exec airflow-webserver python /opt/airflow/scripts/cnpj/check_ingestion_status.py'
"""

import os
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime


def check_status(reference_month="2024-02"):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "osint_metadata"),
        user=os.getenv("POSTGRES_USER", "osint_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "osint_secure_password"),
    )
    cur = conn.cursor(cursor_factory=DictCursor)

    print()
    print("=" * 105)
    print(f"  CNPJ INGESTION — LIVE STATUS ({reference_month})    @ {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 105)

    # ── Manifest summary ──
    cur.execute("""
        SELECT 
            file_type,
            COUNT(*) as total,
            COUNT(CASE WHEN extracted_at IS NOT NULL THEN 1 END) as extracted,
            COUNT(CASE WHEN transformed_at IS NOT NULL THEN 1 END) as transformed,
            COUNT(CASE WHEN loaded_postgres_at IS NOT NULL THEN 1 END) as loaded_pg,
            COUNT(CASE WHEN loaded_neo4j_at IS NOT NULL THEN 1 END) as loaded_neo4j,
            COUNT(CASE WHEN processing_status = 'failed' THEN 1 END) as failed,
            COALESCE(SUM(rows_transformed), 0) as rows_transformed,
            COALESCE(SUM(rows_loaded_postgres), 0) as rows_pg,
            COALESCE(SUM(rows_loaded_neo4j), 0) as rows_neo4j
        FROM cnpj.download_manifest
        WHERE reference_month = %s
        AND file_type IN ('empresas', 'estabelecimentos')
        GROUP BY file_type
        ORDER BY file_type
    """, (reference_month,))

    rows = cur.fetchall()
    print()
    print(f"  MANIFEST TRACKER:")
    print(f"  {'TYPE':<20} {'FILES':>5} {'EXTRD':>6} {'TRANSF':>7} {'PG':>5} {'NEO4J':>6} {'FAIL':>5} | {'ROWS TRANSF':>15} {'ROWS PG':>15} {'ROWS NEO4J':>15}")
    print(f"  {'-'*20} {'-'*5} {'-'*6} {'-'*7} {'-'*5} {'-'*6} {'-'*5} | {'-'*15} {'-'*15} {'-'*15}")
    for r in rows:
        print(
            f"  {r['file_type']:<20} {r['total']:>5} {r['extracted']:>6} {r['transformed']:>7} "
            f"{r['loaded_pg']:>5} {r['loaded_neo4j']:>6} {r['failed']:>5} | "
            f"{r['rows_transformed']:>15,} {r['rows_pg']:>15,} {r['rows_neo4j']:>15,}"
        )

    # ── PostgreSQL actual counts ──
    print()
    print("  POSTGRESQL TABLES:")
    for tbl in ["empresa", "estabelecimento"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM cnpj.{tbl}")
            cnt = cur.fetchone()[0]
            print(f"    cnpj.{tbl:<25s} {cnt:>15,} rows")
        except Exception:
            conn.rollback()
            print(f"    cnpj.{tbl:<25s} (not created yet)")

    # ── Neo4j counts ──
    print()
    print("  NEO4J GRAPH:")
    try:
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
            auth=(
                os.getenv("NEO4J_USER", "neo4j"),
                os.getenv("NEO4J_PASSWORD", "osint_graph_password"),
            ),
        )
        with driver.session() as session:
            for label in ["Empresa", "Estabelecimento"]:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS cnt")
                cnt = result.single()["cnt"]
                print(f"    :{label:<25s} {cnt:>15,} nodes")
            result = session.run("MATCH ()-[r:PERTENCE_A]->() RETURN count(r) AS cnt")
            cnt = result.single()["cnt"]
            print(f"    :{'PERTENCE_A':<25s} {cnt:>15,} relationships")
        driver.close()
    except Exception as e:
        print(f"    (neo4j error: {e})")

    # ── Airflow task states ──
    print()
    print("  AIRFLOW TASKS (latest run):")
    try:
        cur.execute("""
            SELECT task_id, state, 
                   EXTRACT(EPOCH FROM (COALESCE(end_date, NOW()) - start_date))::int as elapsed_s
            FROM task_instance
            WHERE dag_id = 'cnpj_ingestion'
            AND run_id = (
                SELECT run_id FROM dag_run 
                WHERE dag_id = 'cnpj_ingestion' 
                ORDER BY start_date DESC LIMIT 1
            )
            ORDER BY start_date
        """)
        for r in cur.fetchall():
            state_icon = {"success": "✅", "running": "🔄", "failed": "❌", "up_for_retry": "🔁"}.get(r["state"], "⏳")
            elapsed = f"{r['elapsed_s']}s" if r["elapsed_s"] else ""
            print(f"    {state_icon} {r['task_id']:<60s} {r['state'] or 'pending':<15s} {elapsed:>8s}")
    except Exception:
        conn.rollback()
        print("    (could not read task states)")

    print()
    print("=" * 105)
    conn.close()


if __name__ == "__main__":
    import sys
    month = sys.argv[1] if len(sys.argv) > 1 else "2024-02"
    check_status(month)
