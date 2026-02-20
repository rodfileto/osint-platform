#!/bin/bash
# Process all pending CNPJ months by updating config and running DAG tests

MONTHS=(
  "2022-07"
  "2023-03"
  "2023-04"
  "2023-05"
  "2023-06"
  "2023-07"
  "2023-08"
  "2023-09"
  "2023-10"
  "2023-11"
  "2023-12"
  "2024-01"
  "2024-03"
  "2024-04"
  "2024-05"
  "2024-06"
  "2024-07"
  "2024-08"
  "2024-09"
  "2024-10"
  "2024-11"
  "2024-12"
  "2025-01"
  "2025-02"
  "2025-03"
  "2025-04"
  "2025-05"
)

echo "========================================="
echo "CNPJ Batch Processing"
echo "Processing ${#MONTHS[@]} months"
echo "========================================="
echo

for MONTH in "${MONTHS[@]}"; do
    echo "----------------------------------------"
    echo "Processing: $MONTH"
    echo "----------------------------------------"
    
    # Update config file
    docker-compose exec -T airflow-webserver python -c "
from pathlib import Path
import re

config_path = Path('/opt/airflow/dags/tasks/config.py')
content = config_path.read_text()
updated = re.sub(
    r'DEFAULT_REFERENCE_MONTH = \"[^\"]*\"',
    f'DEFAULT_REFERENCE_MONTH = \"$MONTH\"',
    content
)
config_path.write_text(updated)
print(f'✓ Updated config: $MONTH')
"
    
    # Run DAG test
    echo "Running DAG test for $MONTH..."
    docker-compose exec -T airflow-webserver airflow dags test cnpj_ingestion ${MONTH}-01 2>&1 | grep -E "(INFO|ERROR|SUCCESS|FAILED)" | tail -20
    
    echo "✓ Completed: $MONTH"
    echo
done

echo "========================================="
echo "All months processed!"
echo "========================================="
