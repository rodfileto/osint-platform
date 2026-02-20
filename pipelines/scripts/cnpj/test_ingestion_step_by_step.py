#!/usr/bin/env python3
"""
Test CNPJ Ingestion Functions Step by Step

This script allows you to test each stage of the CNPJ ingestion pipeline independently:
1. Extract ZIP files
2. Transform CSV to Parquet (Empresas or Estabelecimentos)
3. Load to PostgreSQL
4. Load to Neo4j

Usage:
    # Test extraction
    python test_ingestion_step_by_step.py extract --zip-file /path/to/Empresas0.zip --output-dir /tmp/test

    # Test transformation (Empresas)
    python test_ingestion_step_by_step.py transform-empresas --csv-file /path/to/file.CSV --output /tmp/test.parquet

    # Test transformation (Estabelecimentos)
    python test_ingestion_step_by_step.py transform-estabelecimentos --csv-file /path/to/file.CSV --output /tmp/test.parquet

    # Test PostgreSQL load
    python test_ingestion_step_by_step.py load-postgres --parquet-files /tmp/file1.parquet /tmp/file2.parquet --table empresas

    # Test Neo4j load
    python test_ingestion_step_by_step.py load-neo4j --parquet-files /tmp/file1.parquet --entity-type Empresa
"""

import sys
import argparse
import logging
from pathlib import Path

# Add DAGs directory to path to import the functions
sys.path.insert(0, '/opt/airflow/dags')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_extract(zip_file: str, output_dir: str):
    """Test ZIP extraction"""
    from cnpj_ingestion_dag import extract_zip_file
    
    logger.info(f"Testing extraction of {zip_file}")
    logger.info(f"Output directory: {output_dir}")
    
    try:
        result = extract_zip_file.function(zip_file, output_dir)
        
        logger.info("✅ Extraction successful!")
        logger.info(f"Extracted files: {result['file_count']}")
        for file in result['extracted_files']:
            file_path = Path(file)
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"  - {file_path.name} ({size_mb:.2f} MB)")
        
        return result
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        raise


def test_transform_empresas(csv_file: str, output_parquet: str):
    """Test Empresas transformation"""
    from cnpj_ingestion_dag import transform_empresas_duckdb
    
    logger.info(f"Testing Empresas transformation")
    logger.info(f"Input CSV: {csv_file}")
    logger.info(f"Output Parquet: {output_parquet}")
    
    # Check if CSV exists
    if not Path(csv_file).exists():
        logger.error(f"❌ CSV file not found: {csv_file}")
        return
    
    try:
        result = transform_empresas_duckdb.function(csv_file, output_parquet)
        
        logger.info("✅ Transformation successful!")
        logger.info(f"Rows transformed: {result['row_count']:,}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info(f"Throughput: {result['throughput_rows_per_sec']:,.0f} rows/sec")
        
        # Check output file
        output_path = Path(output_parquet)
        if output_path.exists():
            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Output file size: {size_mb:.2f} MB")
        
        return result
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def test_transform_estabelecimentos(csv_file: str, output_parquet: str):
    """Test Estabelecimentos transformation"""
    from cnpj_ingestion_dag import transform_estabelecimentos_duckdb
    
    logger.info(f"Testing Estabelecimentos transformation")
    logger.info(f"Input CSV: {csv_file}")
    logger.info(f"Output Parquet: {output_parquet}")
    
    # Check if CSV exists
    if not Path(csv_file).exists():
        logger.error(f"❌ CSV file not found: {csv_file}")
        return
    
    try:
        result = transform_estabelecimentos_duckdb.function(csv_file, output_parquet)
        
        logger.info("✅ Transformation successful!")
        logger.info(f"Rows transformed: {result['row_count']:,}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info(f"Throughput: {result['throughput_rows_per_sec']:,.0f} rows/sec")
        
        # Check output file
        output_path = Path(output_parquet)
        if output_path.exists():
            size_mb = output_path.stat().st_size / (1024 * 1024)
            logger.info(f"Output file size: {size_mb:.2f} MB")
        
        return result
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def test_load_postgres(parquet_files: list[str], table_name: str, schema: str = "cnpj", reference_month: str = "2024-02"):
    """Test PostgreSQL load"""
    from cnpj_ingestion_dag import load_to_postgresql
    
    logger.info(f"Testing PostgreSQL load")
    logger.info(f"Table: {schema}.{table_name}")
    logger.info(f"Files: {len(parquet_files)}")
    logger.info(f"Reference month: {reference_month}")
    
    # Check files exist
    for file in parquet_files:
        if not Path(file).exists():
            logger.error(f"❌ Parquet file not found: {file}")
            return
    
    try:
        result = load_to_postgresql.function(parquet_files, table_name, schema, reference_month)
        
        logger.info("✅ PostgreSQL load successful!")
        logger.info(f"Files loaded: {result['files_loaded']}")
        logger.info(f"Total rows: {result['total_rows']:,}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info(f"Throughput: {result['throughput_rows_per_sec']:,.0f} rows/sec")
        
        return result
    except Exception as e:
        logger.error(f"❌ PostgreSQL load failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def test_load_neo4j(parquet_files: list[str], entity_type: str):
    """Test Neo4j load"""
    from cnpj_ingestion_dag import load_to_neo4j
    
    logger.info(f"Testing Neo4j load")
    logger.info(f"Entity type: {entity_type}")
    logger.info(f"Files: {len(parquet_files)}")
    
    # Check files exist
    for file in parquet_files:
        if not Path(file).exists():
            logger.error(f"❌ Parquet file not found: {file}")
            return
    
    try:
        result = load_to_neo4j.function(parquet_files, entity_type)
        
        logger.info("✅ Neo4j load successful!")
        logger.info(f"Files loaded: {result['files_loaded']}")
        logger.info(f"Total nodes: {result['total_nodes']:,}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info(f"Throughput: {result['throughput_nodes_per_sec']:,.0f} nodes/sec")
        
        return result
    except Exception as e:
        logger.error(f"❌ Neo4j load failed: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    parser = argparse.ArgumentParser(description='Test CNPJ ingestion functions step by step')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Extract command
    extract_parser = subparsers.add_parser('extract', help='Test ZIP extraction')
    extract_parser.add_argument('--zip-file', required=True, help='Path to ZIP file')
    extract_parser.add_argument('--output-dir', required=True, help='Output directory for extracted files')
    
    # Transform empresas command
    transform_emp_parser = subparsers.add_parser('transform-empresas', help='Test Empresas transformation')
    transform_emp_parser.add_argument('--csv-file', required=True, help='Path to CSV file')
    transform_emp_parser.add_argument('--output', required=True, help='Output Parquet file path')
    
    # Transform estabelecimentos command
    transform_est_parser = subparsers.add_parser('transform-estabelecimentos', help='Test Estabelecimentos transformation')
    transform_est_parser.add_argument('--csv-file', required=True, help='Path to CSV file')
    transform_est_parser.add_argument('--output', required=True, help='Output Parquet file path')
    
    # Load PostgreSQL command
    load_pg_parser = subparsers.add_parser('load-postgres', help='Test PostgreSQL load')
    load_pg_parser.add_argument('--parquet-files', nargs='+', required=True, help='Parquet files to load')
    load_pg_parser.add_argument('--table', required=True, choices=['empresas', 'estabelecimentos'], help='Target table')
    load_pg_parser.add_argument('--schema', default='cnpj', help='PostgreSQL schema (default: cnpj)')
    load_pg_parser.add_argument('--reference-month', default='2024-02', help='Reference month (YYYY-MM format)')
    
    # Load Neo4j command
    load_neo_parser = subparsers.add_parser('load-neo4j', help='Test Neo4j load')
    load_neo_parser.add_argument('--parquet-files', nargs='+', required=True, help='Parquet files to load')
    load_neo_parser.add_argument('--entity-type', required=True, choices=['Empresa', 'Estabelecimento'], help='Entity type')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'extract':
            test_extract(args.zip_file, args.output_dir)
        elif args.command == 'transform-empresas':
            test_transform_empresas(args.csv_file, args.output)
        elif args.command == 'transform-estabelecimentos':
            test_transform_estabelecimentos(args.csv_file, args.output)
        elif args.command == 'load-postgres':
            test_load_postgres(args.parquet_files, args.table, args.schema, args.reference_month)
        elif args.command == 'load-neo4j':
            test_load_neo4j(args.parquet_files, args.entity_type)
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
