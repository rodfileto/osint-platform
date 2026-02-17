# Airflow Pipeline Scripts

This directory contains utility scripts for Airflow DAGs and data processing tasks.

## Structure

- `ingestion/` - Data ingestion scripts
- `processing/` - Data transformation and processing scripts
- `utils/` - Utility functions and helpers
- `connectors/` - Database and API connectors

## Usage

Scripts in this directory can be imported and used by DAGs in `/pipelines/dags/`.

Example:
```python
from scripts.utils.database import get_postgres_connection
from scripts.connectors.neo4j_connector import Neo4jConnector
```
