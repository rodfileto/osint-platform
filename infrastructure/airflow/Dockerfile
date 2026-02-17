FROM apache/airflow:2.10.4

# Switch to root to install system dependencies
USER root

# Install system dependencies needed for data processing
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy and install Python dependencies
COPY pipelines/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt || true
