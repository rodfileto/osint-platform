# OSINT Platform Implementation Guide

This guide outlines the concrete steps to continue building the platform from the current state.

## Current Status (Completed)
*   [x] Directory structure created (`backend`, `frontend`, `pipelines`, `infrastructure`).
*   [x] `docker-compose.yml` created with Postgres, Neo4j, Redis, and Airflow.
*   [x] `.env` file created with secure defaults.
*   [x] Postgres initialization script (`init-db.sh`) created for schemas.
*   [x] **Phase 1 Complete**: Backend (Django) initialized and running.
    *   [x] Dockerfile created inside `/backend`.
    *   [x] Django project initialized with `config/` structure.
    *   [x] `requirements.txt` created with dependencies.
    *   [x] Database configured to use PostgreSQL (upgraded to v16).
    *   [x] `apps/` directory created with sys.path configured for direct imports.
    *   [x] Backend service uncommented and tested - running on http://localhost:8000.
    *   [x] Initial migrations applied successfully.
*   [x] **Phase 2 Complete**: Frontend (Next.js) initialized and running.
    *   [x] TailAdmin template cloned into `/frontend`.
    *   [x] Dockerfile created for Next.js with Node 20.
    *   [x] Frontend service uncommented and configured in docker-compose.
    *   [x] Next.js 16 running with Turbopack on http://localhost:3000.
    *   [x] Environment variable `NEXT_PUBLIC_API_URL` configured for backend communication.
*   [x] **Phase 3 Complete**: Airflow & Pipeline Setup initialized.
    *   [x] Airflow webserver, scheduler, and triggerer services started.
    *   [x] Airflow UI accessible at http://localhost:8080 (user: airflow / pass: airflow).
    *   [x] Test connection DAG created in `pipelines/dags/test_connection.py`.
    *   [x] Pipeline structure created with `scripts/` and `plugins/` directories.
    *   [x] Ready for CNPJ pipeline implementation.
*   [x] **CNPJ Data Models**: Django models created and migrated.
    *   [x] Empresa and Estabelecimento models created.
    *   [x] Migrations applied to `cnpj` schema in PostgreSQL.
    *   [x] Models ready for data ingestion.
*   [x] **Step 2B Complete**: Data Cleaning Logic Extracted.
    *   [x] Created `/pipelines/scripts/cnpj/cleaners.py` (650+ lines).
    *   [x] 15+ reusable cleaning functions (CNPJ, currency, dates, strings, etc.).
    *   [x] Pure functions, framework-agnostic, Pandas-compatible.
    *   [x] Comprehensive documentation in `/pipelines/scripts/cnpj/README.md`.
    *   [x] Test suite created in `test_cleaners.py`.
    *   [x] Ready for Airflow DAG integration.
*   [x] **Performance Benchmarking Complete**: DuckDB Transformation Approach Validated.
    *   [x] Created `/pipelines/scripts/cnpj/benchmark_duckdb.py`.
    *   [x] Tested Pure SQL vs Hybrid (SQL + Python UDFs) approaches.
    *   [x] Pure SQL achieved ~4M rows/sec (~3.1s for 12.5M rows).
    *   [x] Confirmed Pure SQL as optimal approach (10-100x faster than Python UDFs).
    *   [x] Validated Parquet intermediate format (70% compression vs CSV).
*   [x] **SQL Transformation Library Complete**: High-Performance DuckDB Cleaners.
    *   [x] Created `/pipelines/scripts/cnpj/cleaners_sql.py` (750+ lines).
    *   [x] Pure SQL equivalents of all Python cleaners.
    *   [x] Complete transformation templates: `empresas_cleaning_template()`, `estabelecimentos_cleaning_template()`.
    *   [x] Query builders: `build_empresas_query()`, `build_estabelecimentos_query()`.
    *   [x] Optimized for DuckDB analytical engine.
*   [x] **Step 2C Complete**: Airflow DAG Implementation.
    *   [x] Created `/pipelines/dags/cnpj_ingestion_dag.py` (580+ lines).
    *   [x] Task groups: `process_empresas_group`, `process_estabelecimentos_group`.
    *   [x] Tasks: Extract ZIP → Transform (DuckDB) → Load (PostgreSQL + Neo4j).
    *   [x] Dynamic task generation for 10 files per entity type (parallel processing).
    *   [x] Parameterized for reference_month configuration.
    *   [x] Schedule: @monthly, max_active_runs=1, retries=2.
    *   [x] Comprehensive documentation in `/pipelines/dags/README_CNPJ_DAG.md` (450+ lines).
*   [x] **Database Schemas Complete**: PostgreSQL and Neo4j Initialization.
    *   [x] Created `/infrastructure/postgres/init-cnpj-schema.sql` (129+ lines).
    *   [x] Tables: `cnpj.empresas`, `cnpj.estabelecimentos`, `cnpj.download_manifest`.
    *   [x] Indexes: Full-text (GIN), geographic, business, temporal.
    *   [x] Views: `download_progress`, `incomplete_months`.
    *   [x] Download manifest table for tracking incremental updates.
    *   [x] Created `/infrastructure/neo4j/init-cnpj-schema.cypher`.
    *   [x] Graph nodes: `:Empresa`, `:Estabelecimento`.
    *   [x] Relationships: `[:PERTENCE_A]`.
    *   [x] Constraints and indexes for performance.
*   [x] **Infrastructure Updates Complete**: Docker and Dependencies.
    *   [x] Updated `docker-compose.yml` with data volume mount (`./data:/opt/airflow/data`).
    *   [x] Updated `/pipelines/requirements.txt` with: duckdb>=1.0.0, pandas>=2.3.0, psycopg2-binary, neo4j>=5.0.0, pyarrow>=14.0.0.
    *   [x] Ready for production deployment.
*   [x] **Documentation Complete**: Implementation Review and Guides.
    *   [x] Created `/CNPJ_IMPLEMENTATION_REVIEW.md` - Complete review checklist.
    *   [x] Created `/pipelines/dags/README_CNPJ_DAG.md` - Usage guide (450+ lines).
    *   [x] Created `/pipelines/scripts/cnpj/README.md` - Cleaners module documentation.
    *   [x] Execution commands documented (ready but not yet executed).
*   [ ] **Historical Data Copy**: In Progress (86% Complete).
    *   [x] Initiated background rsync copy: 174GB, 1,170 ZIP files, 36 months (2022-08 to 2025-06).
    *   [x] Progress: 150GB/174GB copied, 1,021/1,170 files (~86%).
    *   [x] Currently copying: 2025-04 month.
    *   [x] Remaining: ~24GB, ~149 files (2 months: 2025-05, 2025-06).
    *   [x] ETA: 5-10 minutes.
    *   [x] Process running: 5+ hours (rsync background job).

## Next Steps (Ready to Execute)

### Priority 1: Complete Data Copy (5-10 minutes)
1.  **Monitor Copy Completion**
    ```bash
    # Check progress
    du -sh /media/mynewdrive/osint-platform/data/cnpj/raw/  # Target: ~174GB
    find /media/mynewdrive/osint-platform/data/cnpj/raw/ -name "*.zip" | wc -l  # Target: 1,170
    tail -20 /tmp/cnpj_full_copy.log  # Watch real-time progress
    ```

### Priority 2: Start Docker Services
1.  **Launch Infrastructure**
    ```bash
    cd /media/mynewdrive/osint-platform
    docker-compose up -d postgres neo4j redis airflow-webserver airflow-scheduler airflow-init
    docker-compose ps  # Verify all services healthy
    ```

2.  **Initialize Database Schemas**
    ```bash
    # PostgreSQL (may auto-initialize via init-db.sh)
    docker-compose exec postgres psql -U osint_user -d osint_platform -f /docker-entrypoint-initdb.d/init-cnpj-schema.sql
    
    # Neo4j
    cat infrastructure/neo4j/init-cnpj-schema.cypher | docker-compose exec -T neo4j cypher-shell -u neo4j -p osint_password
    ```

3.  **Verify Airflow DAG Loaded**
    ```bash
    docker-compose exec airflow-scheduler airflow dags list | grep cnpj_ingestion
    ```

### Priority 3: Test with Single Month (2024-02)
1.  **Trigger Test Run**
    ```bash
    docker-compose exec airflow-scheduler airflow dags trigger cnpj_ingestion \
      --conf '{"reference_month": "2024-02"}'
    ```

2.  **Monitor Execution**
    ```bash
    # Watch logs
    docker-compose logs -f airflow-scheduler | grep -E "(empresas|estabelecimentos|Success|Error)"
    
    # Check Airflow UI
    # http://localhost:8080
    ```

3.  **Verify Data Loaded**
    ```sql
    -- PostgreSQL (via psql or DBeaver)
    SELECT reference_month, COUNT(*) FROM cnpj.empresas GROUP BY reference_month;
    SELECT reference_month, COUNT(*) FROM cnpj.estabelecimentos GROUP BY reference_month;
    SELECT * FROM cnpj.download_progress WHERE reference_month = '2024-02';
    
    -- Neo4j (via Browser: http://localhost:7474)
    MATCH (e:Empresa) RETURN COUNT(e);
    MATCH (est:Estabelecimento)-[:PERTENCE_A]->(e:Empresa) RETURN COUNT(est);
    ```

### Priority 4: Backfill All 36 Months
1.  **Full Historical Processing**
    ```bash
    # Option 1: Trigger all months sequentially (Airflow will respect schedule)
    for month in 2022-{08..12} 2023-{01..12} 2024-{01..12} 2025-{01..06}; do
      docker-compose exec airflow-scheduler airflow dags trigger cnpj_ingestion \
        --conf "{\"reference_month\": \"$month\"}"
      sleep 10  # Small delay between triggers
    done
    
    # Option 2: Use Airflow backfill command
    docker-compose exec airflow-scheduler airflow dags backfill cnpj_ingestion \
      --start-date 2022-08-01 --end-date 2025-06-30
    ```

2.  **Expected Performance**
    *   Duration: 5-8 minutes per month
    *   Total time: ~3-4 hours for 36 months
    *   Throughput: ~4M rows/sec transformation
    *   Storage: ~180GB Parquet output

### Optional: Step 2A - Pipeline Dependencies (Already Complete)
1.  ✅ **requirements.txt Updated**
    *   duckdb>=1.0.0
    *   pandas>=2.3.0
    *   psycopg2-binary
    *   neo4j>=5.0.0
    *   pyarrow>=14.0.0

### Phase 4: Development Workflow
1.  **Start All Services**: `docker-compose up -d`.
2.  **Access Points**:
    *   Frontend: http://localhost:3000
    *   Backend API: http://localhost:8000
    *   Airflow UI: http://localhost:8080
    *   Neo4j Browser: http://localhost:7474
3.  **Data Ingestion**:
    *   Write scripts in `pipelines/scripts/` to load sample data.
    *   Trigger them via Airflow.

## Entity Resolution Strategy
When implementing the ETL pipelines, remember the "Golden Rule":
*   Check if the Entity (CPF/CNPJ) exists in the **Global Graph** first.
*   If yes -> Link to it.
*   If no -> Create new Node.
