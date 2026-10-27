FROM apache/airflow:2.10.4

# Switch to root to install system dependencies
USER root

# Install system dependencies needed for data processing
RUN apt-get update && apt-get install -y \
    build-essential \
    procps \
    && rm -rf /var/lib/apt/lists/*

# dbt runs in its own virtualenv so its dependencies never clash with Airflow's
# constraints; only the dbt command is put on the PATH (the DAG's BashOperator calls it).
COPY pipelines/dbt/requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY pipelines/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
