# Pipeline Scripts

Source-specific code used by the Airflow DAGs in `pipelines/dags/`. The scripts are mounted at `/opt/airflow/scripts` and the DAGs add the folder they need to `sys.path`.

## Structure

- `cnpj/` - Receita Federal download (`downloader.py`), download manifest (`manifest_tracker.py`) and field cleaning (`cleaners.py`). The task logic of `cnpj_pipeline` lives in `pipelines/dags/tasks/`.
- `finep/` - FINEP spreadsheet download and load (`downloader.py`, `loader.py`) and the experimental BERTopic pipeline of `finep_nlp_topic` (`nlp_topic_pipeline.py`).
- `inpi/` - INPI patent CSV download (`downloader.py`) and the staged, atomic load into `inpi.*` (`loader.py`).
- `geo/` - Brazilian geography reference loader. It is not Airflow-only: it also runs as a standalone script from the dev bootstrap and the `geo-bootstrap` service, so keep it executable on its own.

## Usage

```python
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from inpi import loader
from inpi.downloader import INPI_DATASETS, collect_dataset
```
