FROM apache/airflow:2.10.4

# Switch to root to install system dependencies
USER root

# Install system dependencies needed for data processing
RUN apt-get update && apt-get install -y \
    build-essential \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Directory and PATH entry for the dbt virtualenv, created below as the airflow user.
# Python must not run as root here: root's interpreter also loads the airflow user's
# site-packages and leaves root-owned __pycache__ files that break the pip install below.
RUN mkdir /opt/dbt-venv \
    && chown airflow:root /opt/dbt-venv \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY pipelines/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt runs in its own virtualenv so its dependencies never clash with Airflow's
# constraints; only the dbt command is on the PATH (the DAG's BashOperator calls it).
COPY pipelines/dbt/requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt
