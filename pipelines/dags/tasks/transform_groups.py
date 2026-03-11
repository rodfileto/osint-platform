"""
CNPJ Transform Groups

Task groups and individual processing tasks for the extract→transform pipeline.
Handles Empresas, Estabelecimentos, Socios, Simples, and reference tables.
"""

import logging
import time
from airflow.decorators import task, task_group

from .extract_tasks import extract_zip_file
from .transform_tasks import (
    transform_empresas_duckdb,
    transform_estabelecimentos_duckdb,
    transform_socios_duckdb,
    transform_simples_duckdb,
    transform_reference_duckdb,
)
from .config import RAW_PATH, STAGING_PATH, PROCESSED_PATH

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Reference file mapping: zip stem -> (ref_type, output parquet name)
# ---------------------------------------------------------------------------
REFERENCE_FILES = {
    'Cnaes':       ('cnaes',        'cnaes.parquet'),
    'Motivos':     ('motivos',      'motivos.parquet'),
    'Municipios':  ('municipios',   'municipios.parquet'),
    'Naturezas':   ('naturezas',    'naturezas.parquet'),
    'Paises':      ('paises',       'paises.parquet'),
    'Qualificacoes': ('qualificacoes', 'qualificacoes.parquet'),
}


# ============================================================================
# EMPRESAS
# ============================================================================

@task
def process_empresas_file(reference_month: str, file_name: str, **context) -> dict | None:
    """
    Process a single Empresas file: extract, transform, and return result.
    Updates manifest at each stage to track progress.

    Args:
        reference_month: YYYY-MM format
        file_name: Name of the ZIP file
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )

    force_reprocess = context.get('params', {}).get('force_reprocess', False)

    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
        logger.info(f"{file_name} already transformed, skipping")
        file_num = file_name.replace('Empresas', '').replace('.zip', '')
        parquet_file = PROCESSED_PATH / reference_month / f"empresas_{file_num}.parquet"
        if parquet_file.exists():
            return {"parquet_path": str(parquet_file), "skipped": True}
        return None

    try:
        file_num = file_name.replace('Empresas', '').replace('.zip', '')
        zip_file = RAW_PATH / reference_month / file_name
        staging_dir = STAGING_PATH / reference_month / f"empresas_{file_num}"
        parquet_file = PROCESSED_PATH / reference_month / f"empresas_{file_num}.parquet"

        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(
                output_dir=str(staging_dir),
                zip_path=str(zip_file),
                reference_month=reference_month,
                file_name=file_name,
            )
            csv_path = extract_info["extracted_files"][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            csv_files = list(staging_dir.glob('*.CSV')) + list(staging_dir.glob('*.EMPRECSV'))
            csv_path = str(csv_files[0]) if csv_files else None

        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")

        start_time = time.time()
        transform_info = transform_empresas_duckdb.function(csv_path, str(parquet_file))
        duration = time.time() - start_time

        mark_transformed(
            reference_month, file_name,
            transform_info["output_parquet"], transform_info["row_count"], duration,
        )

        return {
            "parquet_path": transform_info["output_parquet"],
            "row_count": transform_info["row_count"],
            "skipped": False,
        }

    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def transform_empresas_group():
    """
    Extract and transform all Empresas files for a given reference month.
    Dynamically discovers files from manifest.
    """
    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)

    @task
    def get_empresas_files(reference_month: str, **context) -> list:
        """Get list of Empresas files to process from manifest."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process, get_files_for_reprocessing

        force_reprocess = context['params'].get('force_reprocess', False)

        if force_reprocess:
            files = get_files_for_reprocessing(reference_month, file_type='empresas', require_parquet=True)
        else:
            files = get_files_to_process(reference_month, file_type='empresas', stage='transformed')

        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Empresas files to process: {file_names}")
        return file_names

    ref_month = get_ref_month()
    files_to_process = get_empresas_files(ref_month)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    process_empresas_file.partial(reference_month=ref_month).expand(file_name=files_to_process)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


# ============================================================================
# ESTABELECIMENTOS
# ============================================================================

@task
def process_estabelecimentos_file(reference_month: str, file_name: str, **context) -> dict | None:
    """
    Process a single Estabelecimentos file: extract, transform, and return result.
    Updates manifest at each stage to track progress.

    Args:
        reference_month: YYYY-MM format
        file_name: Name of the ZIP file
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )

    force_reprocess = context.get('params', {}).get('force_reprocess', False)

    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
        logger.info(f"{file_name} already transformed, skipping")
        file_num = file_name.replace('Estabelecimentos', '').replace('.zip', '')
        parquet_file = PROCESSED_PATH / reference_month / f"estabelecimentos_{file_num}.parquet"
        if parquet_file.exists():
            return {"parquet_path": str(parquet_file), "skipped": True}
        return None

    try:
        file_num = file_name.replace('Estabelecimentos', '').replace('.zip', '')
        zip_file = RAW_PATH / reference_month / file_name
        staging_dir = STAGING_PATH / reference_month / f"estabelecimentos_{file_num}"
        parquet_file = PROCESSED_PATH / reference_month / f"estabelecimentos_{file_num}.parquet"

        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(
                output_dir=str(staging_dir),
                zip_path=str(zip_file),
                reference_month=reference_month,
                file_name=file_name,
            )
            csv_path = extract_info["extracted_files"][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            csv_files = list(staging_dir.glob('*.CSV')) + list(staging_dir.glob('*.ESTABELE'))
            csv_path = str(csv_files[0]) if csv_files else None

        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")

        start_time = time.time()
        transform_info = transform_estabelecimentos_duckdb.function(csv_path, str(parquet_file))
        duration = time.time() - start_time

        mark_transformed(
            reference_month, file_name,
            transform_info["output_parquet"], transform_info["row_count"], duration,
        )

        return {
            "parquet_path": transform_info["output_parquet"],
            "row_count": transform_info["row_count"],
            "skipped": False,
        }

    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def transform_estabelecimentos_group():
    """
    Extract and transform all Estabelecimentos files for a given reference month.
    Dynamically discovers files from manifest.
    """
    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)

    @task
    def get_estabelecimentos_files(reference_month: str, **context) -> list:
        """Get list of Estabelecimentos files to process from manifest."""
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process, get_files_for_reprocessing

        force_reprocess = context['params'].get('force_reprocess', False)

        if force_reprocess:
            files = get_files_for_reprocessing(reference_month, file_type='estabelecimentos', require_parquet=True)
        else:
            files = get_files_to_process(reference_month, file_type='estabelecimentos', stage='transformed')

        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Estabelecimentos files to process: {file_names}")
        return file_names

    ref_month = get_ref_month()
    files_to_process = get_estabelecimentos_files(ref_month)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    process_estabelecimentos_file.partial(reference_month=ref_month).expand(file_name=files_to_process)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


# ============================================================================
# SOCIOS
# ============================================================================

@task
def process_socios_file(reference_month: str, file_name: str, **context) -> dict | None:
    """
    Process a single Socios file: extract, transform, return result.
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )

    force_reprocess = context.get('params', {}).get('force_reprocess', False)

    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
        logger.info(f"{file_name} already transformed, skipping")
        file_num = file_name.replace('Socios', '').replace('.zip', '')
        parquet_file = PROCESSED_PATH / reference_month / f"socios_{file_num}.parquet"
        if parquet_file.exists():
            return {"parquet_path": str(parquet_file), "skipped": True}
        return None

    try:
        file_num = file_name.replace('Socios', '').replace('.zip', '')
        zip_file = RAW_PATH / reference_month / file_name
        staging_dir = STAGING_PATH / reference_month / f"socios_{file_num}"
        parquet_file = PROCESSED_PATH / reference_month / f"socios_{file_num}.parquet"

        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(
                output_dir=str(staging_dir),
                zip_path=str(zip_file),
                reference_month=reference_month,
                file_name=file_name,
            )
            csv_path = extract_info["extracted_files"][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            csv_files = (
                list(staging_dir.glob('*.CSV'))
                + list(staging_dir.glob('*.SOCIOCSV'))
                + list(staging_dir.glob('*'))
            )
            csv_files = [f for f in csv_files if f.is_file()]
            csv_path = str(csv_files[0]) if csv_files else None

        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")

        start_time = time.time()
        transform_info = transform_socios_duckdb.function(csv_path, str(parquet_file))
        duration = time.time() - start_time

        mark_transformed(
            reference_month, file_name,
            transform_info["output_parquet"], transform_info["row_count"], duration,
        )

        return {
            "parquet_path": transform_info["output_parquet"],
            "row_count": transform_info["row_count"],
            "skipped": False,
        }

    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def transform_socios_group():
    """
    Extract and transform all Socios files (Socios0.zip … Socios9.zip)
    for a given reference month.
    """
    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)

    @task
    def get_socios_files(reference_month: str, **context) -> list:
        import sys
        sys.path.insert(0, '/opt/airflow/scripts/cnpj')
        from manifest_tracker import get_files_to_process, get_files_for_reprocessing

        force_reprocess = context['params'].get('force_reprocess', False)

        if force_reprocess:
            files = get_files_for_reprocessing(reference_month, file_type='socios', require_parquet=True)
        else:
            files = get_files_to_process(reference_month, file_type='socios', stage='transformed')

        file_names = [f['file_name'] for f in files]
        logger.info(f"Found {len(file_names)} Socios files to process: {file_names}")
        return file_names

    ref_month = get_ref_month()
    files_to_process = get_socios_files(ref_month)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]

    process_socios_file.partial(reference_month=ref_month).expand(file_name=files_to_process)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


# ============================================================================
# SIMPLES
# ============================================================================

@task
def process_simples_file(reference_month: str, **context) -> dict | None:
    """
    Process the single Simples.zip file for a reference month.
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )

    file_name = 'Simples.zip'
    force_reprocess = context.get('params', {}).get('force_reprocess', False)

    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
        parquet_file = PROCESSED_PATH / reference_month / 'simples.parquet'
        if parquet_file.exists():
            return {'parquet_path': str(parquet_file), 'skipped': True}
        return None

    try:
        zip_file = RAW_PATH / reference_month / file_name
        # NOTE: Extract phase creates 'simples_' (with underscore but no number) due to empty file_num
        staging_dir = STAGING_PATH / reference_month / 'simples_'
        parquet_file = PROCESSED_PATH / reference_month / 'simples.parquet'

        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(
                output_dir=str(staging_dir),
                zip_path=str(zip_file),
                reference_month=reference_month,
                file_name=file_name,
            )
            csv_path = extract_info['extracted_files'][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            csv_files = [f for f in staging_dir.rglob('*') if f.is_file() and f.suffix.upper() in ['.CSV', '.SIMPLES']]
            if not csv_files:
                csv_files = [f for f in staging_dir.rglob('*') if f.is_file()]
            csv_path = str(csv_files[0]) if csv_files else None

        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")

        start_time = time.time()
        transform_info = transform_simples_duckdb.function(csv_path, str(parquet_file))
        duration = time.time() - start_time

        mark_transformed(
            reference_month, file_name,
            transform_info['output_parquet'], transform_info['row_count'], duration,
        )

        return {
            'parquet_path': transform_info['output_parquet'],
            'row_count': transform_info['row_count'],
            'skipped': False,
        }

    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def transform_simples_group():
    """Extract and transform Simples.zip for a given reference month."""

    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)

    ref_month = get_ref_month()
    process_simples_file(ref_month)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]


# ============================================================================
# REFERENCE TABLES
# ============================================================================

@task
def process_reference_file(reference_month: str, zip_stem: str, **context) -> dict | None:
    """
    Process a single reference table ZIP (Cnaes, Motivos, Municipios, etc.).

    Args:
        reference_month: YYYY-MM format
        zip_stem: Stem of the ZIP file (e.g. 'Cnaes', 'Motivos')
    """
    import sys
    sys.path.insert(0, '/opt/airflow/scripts/cnpj')
    from manifest_tracker import (
        is_file_processed, mark_extracted, mark_transformed, mark_failed
    )

    ref_type, parquet_name = REFERENCE_FILES[zip_stem]
    file_name = f'{zip_stem}.zip'
    force_reprocess = context.get('params', {}).get('force_reprocess', False)

    if not force_reprocess and is_file_processed(reference_month, file_name, 'transformed'):
        parquet_file = PROCESSED_PATH / reference_month / parquet_name
        if parquet_file.exists():
            return {'parquet_path': str(parquet_file), 'ref_type': ref_type, 'skipped': True}
        return None

    try:
        zip_file = RAW_PATH / reference_month / file_name
        # NOTE: Extract phase creates 'references_<ZipStem>' directories
        staging_dir = STAGING_PATH / reference_month / f"references_{zip_stem}"
        parquet_file = PROCESSED_PATH / reference_month / parquet_name

        if not is_file_processed(reference_month, file_name, 'extracted'):
            extract_info = extract_zip_file.function(
                output_dir=str(staging_dir),
                zip_path=str(zip_file),
                reference_month=reference_month,
                file_name=file_name,
            )
            csv_path = extract_info['extracted_files'][0]
            mark_extracted(reference_month, file_name, csv_path)
        else:
            csv_files = [f for f in staging_dir.rglob('*') if f.is_file() and f.suffix.upper() == '.CSV']
            if not csv_files:
                csv_files = [f for f in staging_dir.rglob('*') if f.is_file()]
            csv_path = str(csv_files[0]) if csv_files else None

        if not csv_path:
            raise ValueError(f"CSV file not found for {file_name}")

        start_time = time.time()
        transform_info = transform_reference_duckdb.function(csv_path, str(parquet_file), ref_type)
        duration = time.time() - start_time

        mark_transformed(
            reference_month, file_name,
            transform_info['output_parquet'], transform_info['row_count'], duration,
        )

        return {
            'parquet_path': transform_info['output_parquet'],
            'ref_type': ref_type,
            'row_count': transform_info['row_count'],
            'skipped': False,
        }

    except Exception as e:
        mark_failed(reference_month, file_name, str(e))
        logger.error(f"Failed to process {file_name}: {e}")
        raise


@task_group
def transform_references_group():
    """
    Extract and transform all 6 reference table ZIPs for a given reference month.
    Files run sequentially (they are small and fast).
    """

    @task
    def get_ref_month(**context) -> str:
        from .config import DEFAULT_REFERENCE_MONTH
        return context['params'].get('reference_month', DEFAULT_REFERENCE_MONTH)

    @task
    def run_all_references(reference_month: str, **context) -> dict:
        results = {}
        for zip_stem in REFERENCE_FILES:
            try:
                result = process_reference_file.function(
                    reference_month=reference_month,
                    zip_stem=zip_stem,
                    **context,
                )
                results[zip_stem] = result
                logger.info(f"  {zip_stem}: {result}")
            except Exception as e:
                logger.error(f"  {zip_stem} failed: {e}")
                results[zip_stem] = {'error': str(e)}
        return results

    ref_month = get_ref_month()
    run_all_references(ref_month)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
