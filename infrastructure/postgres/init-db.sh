#!/bin/bash
set -e

# Setup Airflow DB
# Note: Airflow manages its own schema tables, but we ensure the DB exists if not default
# (In this setup, we use the main 'osint_metadata' DB for airflow too, or we can create a separate one)

# Create Schemas for OSINT Modules
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS airflow;
    CREATE SCHEMA IF NOT EXISTS naturalization;
    CREATE SCHEMA IF NOT EXISTS cnpj;
    CREATE SCHEMA IF NOT EXISTS sanctions;
    CREATE SCHEMA IF NOT EXISTS contracts;
    
    -- Grant usage to user (if we had separate restricted users)
    GRANT ALL PRIVILEGES ON SCHEMA airflow TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA naturalization TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA cnpj TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA sanctions TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON SCHEMA contracts TO $POSTGRES_USER;
EOSQL
