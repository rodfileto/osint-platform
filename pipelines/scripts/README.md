# Airflow Pipeline Scripts

This directory contains utility scripts for Airflow DAGs and data processing tasks.

## Structure

- `ingestion/` - Data ingestion scripts
- `processing/` - Data transformation and processing scripts
- `utils/` - Utility functions and helpers
- `connectors/` - Database and API connectors
- `geo/` - Shared geography ingestion scripts used by dev bootstrap today and reusable by future production jobs

## Usage

Scripts in this directory can be imported and used by DAGs in `/pipelines/dags/`.

The `geo/` folder is intentionally not Airflow-only. Shared loaders there should remain executable as standalone scripts so dev bootstrap, ad hoc production runs, and future DAG wrappers can reuse the same logic.

Example:
```python
from scripts.utils.database import get_postgres_connection
from scripts.connectors.neo4j_connector import Neo4jConnector
```
