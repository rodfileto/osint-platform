"""
CNPJ Transformation Tasks

Tasks for transforming CNPJ CSV data using DuckDB SQL.
"""

import logging
import time
import sys
from pathlib import Path
import duckdb
from airflow.decorators import task

# Add scripts to path
sys.path.insert(0, '/opt/airflow/scripts/cnpj')

logger = logging.getLogger(__name__)


@task
def transform_empresas_duckdb(csv_path: str, output_parquet: str) -> dict:
    """
    Transform Empresas CSV using DuckDB pure SQL.
    
    Uses SQL cleaners from cleaners module for maximum performance.
    
    Args:
        csv_path: Path to extracted CSV file
        output_parquet: Output Parquet file path
        
    Returns:
        Dict with transformation stats
    """
    from cleaners import build_empresas_query
    
    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if output already exists and is valid
    if output_parquet.exists() and output_parquet.stat().st_size > 0:
        logger.info(f"Output file {output_parquet.name} already exists, reading row count")
        try:
            con = duckdb.connect()
            row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
            con.close()
            logger.info(f"  Found existing file with {row_count:,} rows")
            return {
                "input_csv": str(csv_path),
                "output_parquet": str(output_parquet),
                "row_count": row_count,
                "duration_seconds": 0,
                "throughput_rows_per_sec": 0
            }
        except Exception as e:
            logger.warning(f"Existing file corrupted, will recreate: {e}")
            output_parquet.unlink()
    
    # Optimize: Use SSD for temp storage if available
    temp_dir = Path("/opt/airflow/temp_ssd")
    if not temp_dir.exists():
        logger.warning(f"SSD temp dir {temp_dir} not found, falling back to data dir")
        temp_dir = Path("/opt/airflow/data/cnpj/temp")
    
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Transforming {csv_path.name} with DuckDB SQL using {temp_dir}")
    start_time = time.time()
    
    # Connect to DuckDB with optimized configuration (10GB RAM limit + SSD spilling)
    con = duckdb.connect(config={'temp_directory': str(temp_dir), 'memory_limit': '10GB'})
    
    try:
        # Build and execute transformation query
        query = build_empresas_query(str(csv_path), str(output_parquet))
        con.execute(query)
        
        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
        
        duration = time.time() - start_time
        throughput = row_count / duration if duration > 0 else 0
        
        logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
        
        return {
            "input_csv": str(csv_path),
            "output_parquet": str(output_parquet),
            "row_count": row_count,
            "duration_seconds": duration,
            "throughput_rows_per_sec": throughput
        }
    finally:
        # Always close connection to release locks
        con.close()


@task
def transform_estabelecimentos_duckdb(csv_path: str, output_parquet: str) -> dict:
    """
    Transform Estabelecimentos CSV using DuckDB pure SQL.
    
    Args:
        csv_path: Path to extracted CSV file
        output_parquet: Output Parquet file path
        
    Returns:
        Dict with transformation stats
    """
    from cleaners import build_estabelecimentos_query
    
    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if output already exists and is valid
    if output_parquet.exists() and output_parquet.stat().st_size > 0:
        logger.info(f"Output file {output_parquet.name} already exists, reading row count")
        try:
            con = duckdb.connect()
            row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
            con.close()
            logger.info(f"  Found existing file with {row_count:,} rows")
            return {
                "input_csv": str(csv_path),
                "output_parquet": str(output_parquet),
                "row_count": row_count,
                "duration_seconds": 0,
                "throughput_rows_per_sec": 0
            }
        except Exception as e:
            logger.warning(f"Existing file corrupted, will recreate: {e}")
            output_parquet.unlink()
    
    # Optimize: Use SSD for temp storage if available
    temp_dir = Path("/opt/airflow/temp_ssd")
    if not temp_dir.exists():
        logger.warning(f"SSD temp dir {temp_dir} not found, falling back to data dir")
        temp_dir = Path("/opt/airflow/data/cnpj/temp")
    
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Transforming {csv_path.name} with DuckDB SQL using {temp_dir}")
    start_time = time.time()
    
    # Connect to DuckDB with optimized configuration (10GB RAM limit + SSD spilling)
    con = duckdb.connect(config={'temp_directory': str(temp_dir), 'memory_limit': '10GB'})
    
    try:
        query = build_estabelecimentos_query(str(csv_path), str(output_parquet))
        con.execute(query)
        
        row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
        duration = time.time() - start_time
        throughput = row_count / duration if duration > 0 else 0
        
        logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
        
        return {
            "input_csv": str(csv_path),
            "output_parquet": str(output_parquet),
            "row_count": row_count,
            "duration_seconds": duration,
            "throughput_rows_per_sec": throughput
        }
    finally:
        # Always close connection to release locks
        con.close()


@task
def transform_socios_duckdb(csv_path: str, output_parquet: str) -> dict:
    """
    Transform Socios CSV using DuckDB pure SQL.

    Args:
        csv_path: Path to extracted CSV file (from SociosN.zip)
        output_parquet: Output Parquet file path

    Returns:
        Dict with transformation stats
    """
    from cleaners import build_socios_query

    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)

    if output_parquet.exists() and output_parquet.stat().st_size > 0:
        logger.info(f"Output file {output_parquet.name} already exists, reading row count")
        try:
            con = duckdb.connect()
            row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
            con.close()
            logger.info(f"  Found existing file with {row_count:,} rows")
            return {
                "input_csv": str(csv_path),
                "output_parquet": str(output_parquet),
                "row_count": row_count,
                "duration_seconds": 0,
                "throughput_rows_per_sec": 0,
            }
        except Exception as e:
            logger.warning(f"Existing file corrupted, will recreate: {e}")
            output_parquet.unlink()

    temp_dir = Path("/opt/airflow/temp_ssd")
    if not temp_dir.exists():
        temp_dir = Path("/opt/airflow/data/cnpj/temp")
    temp_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Transforming {csv_path.name} (Socios) with DuckDB SQL")
    start_time = time.time()

    con = duckdb.connect(config={'temp_directory': str(temp_dir), 'memory_limit': '10GB'})
    try:
        query = build_socios_query(str(csv_path), str(output_parquet))
        con.execute(query)
        row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
        duration = time.time() - start_time
        throughput = row_count / duration if duration > 0 else 0
        logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
        return {
            "input_csv": str(csv_path),
            "output_parquet": str(output_parquet),
            "row_count": row_count,
            "duration_seconds": duration,
            "throughput_rows_per_sec": throughput,
        }
    finally:
        con.close()


@task
def transform_simples_duckdb(csv_path: str, output_parquet: str) -> dict:
    """
    Transform Simples Nacional CSV using DuckDB pure SQL.

    Args:
        csv_path: Path to extracted CSV file (from Simples.zip)
        output_parquet: Output Parquet file path

    Returns:
        Dict with transformation stats
    """
    from cleaners import build_simples_query

    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)

    if output_parquet.exists() and output_parquet.stat().st_size > 0:
        logger.info(f"Output file {output_parquet.name} already exists, reading row count")
        try:
            con = duckdb.connect()
            row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
            con.close()
            return {
                "input_csv": str(csv_path),
                "output_parquet": str(output_parquet),
                "row_count": row_count,
                "duration_seconds": 0,
                "throughput_rows_per_sec": 0,
            }
        except Exception as e:
            logger.warning(f"Existing file corrupted, will recreate: {e}")
            output_parquet.unlink()

    temp_dir = Path("/opt/airflow/temp_ssd")
    if not temp_dir.exists():
        temp_dir = Path("/opt/airflow/data/cnpj/temp")
    temp_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Transforming {csv_path.name} (Simples) with DuckDB SQL")
    start_time = time.time()

    con = duckdb.connect(config={'temp_directory': str(temp_dir), 'memory_limit': '10GB'})
    try:
        query = build_simples_query(str(csv_path), str(output_parquet))
        con.execute(query)
        row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
        duration = time.time() - start_time
        throughput = row_count / duration if duration > 0 else 0
        logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s ({throughput:,.0f} rows/sec)")
        return {
            "input_csv": str(csv_path),
            "output_parquet": str(output_parquet),
            "row_count": row_count,
            "duration_seconds": duration,
            "throughput_rows_per_sec": throughput,
        }
    finally:
        con.close()


@task
def transform_reference_duckdb(csv_path: str, output_parquet: str, ref_type: str) -> dict:
    """
    Transform a reference table CSV (Cnaes, Motivos, Municipios, Naturezas, Paises,
    Qualificacoes) using DuckDB.

    Args:
        csv_path: Path to extracted CSV file
        output_parquet: Output Parquet file path
        ref_type: One of 'cnaes', 'motivos', 'municipios', 'naturezas', 'paises', 'qualificacoes'

    Returns:
        Dict with transformation stats
    """
    from cleaners import build_reference_query

    csv_path = Path(csv_path)
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)

    if output_parquet.exists() and output_parquet.stat().st_size > 0:
        logger.info(f"Output file {output_parquet.name} already exists, reading row count")
        try:
            con = duckdb.connect()
            row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
            con.close()
            return {
                "input_csv": str(csv_path),
                "output_parquet": str(output_parquet),
                "ref_type": ref_type,
                "row_count": row_count,
                "duration_seconds": 0,
            }
        except Exception as e:
            logger.warning(f"Existing file corrupted, will recreate: {e}")
            output_parquet.unlink()

    logger.info(f"Transforming {csv_path.name} (reference/{ref_type}) with DuckDB SQL")
    start_time = time.time()

    con = duckdb.connect()
    try:
        query = build_reference_query(str(csv_path), str(output_parquet), ref_type)
        con.execute(query)
        row_count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
        duration = time.time() - start_time
        logger.info(f"  Transformed {row_count:,} rows in {duration:.2f}s")
        return {
            "input_csv": str(csv_path),
            "output_parquet": str(output_parquet),
            "ref_type": ref_type,
            "row_count": row_count,
            "duration_seconds": duration,
        }
    finally:
        con.close()

