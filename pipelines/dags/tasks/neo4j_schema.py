"""
Neo4j Schema Management Tasks

Tasks for creating and managing Neo4j indexes and constraints.
"""

import logging
from airflow.decorators import task
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


@task
def ensure_neo4j_indexes(**context) -> dict:
    """
    Ensure all required Neo4j indexes and constraints exist.
    
    Creates indexes if they don't exist (IF NOT EXISTS is idempotent).
    Indexes build in background and don't block data loading.
    
    Returns:
        Dict with created indexes/constraints count
    """
    from .config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    
    logger.info("Ensuring Neo4j indexes and constraints...")
    
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    # Define all indexes and constraints from init-cnpj-schema.cypher
    statements = [
        # ===== CONSTRAINTS =====
        "CREATE CONSTRAINT empresa_cnpj_basico IF NOT EXISTS FOR (e:Empresa) REQUIRE e.cnpj_basico IS UNIQUE",
        "CREATE CONSTRAINT pessoa_id IF NOT EXISTS FOR (p:Pessoa) REQUIRE p.pessoa_id IS UNIQUE",

        # ===== EMPRESA INDEXES =====
        "CREATE INDEX empresa_razao_social IF NOT EXISTS FOR (e:Empresa) ON (e.razao_social)",
        "CREATE INDEX empresa_porte IF NOT EXISTS FOR (e:Empresa) ON (e.porte_empresa)",
        "CREATE INDEX empresa_natureza IF NOT EXISTS FOR (e:Empresa) ON (e.natureza_juridica)",
        "CREATE INDEX empresa_created_at IF NOT EXISTS FOR (e:Empresa) ON (e.created_at)",
        "CREATE INDEX empresa_updated_at IF NOT EXISTS FOR (e:Empresa) ON (e.updated_at)",
        "CREATE FULLTEXT INDEX empresa_names IF NOT EXISTS FOR (e:Empresa) ON EACH [e.razao_social]",

        # ===== PESSOA INDEXES =====
        "CREATE INDEX pessoa_cpf_cnpj IF NOT EXISTS FOR (p:Pessoa) ON (p.cpf_cnpj_socio)",
        "CREATE INDEX pessoa_identificador IF NOT EXISTS FOR (p:Pessoa) ON (p.identificador_socio)",
        "CREATE INDEX pessoa_faixa_etaria IF NOT EXISTS FOR (p:Pessoa) ON (p.faixa_etaria)",
        "CREATE FULLTEXT INDEX pessoa_names IF NOT EXISTS FOR (p:Pessoa) ON EACH [p.nome]",

        # ===== RELATIONSHIP INDEXES =====
        "CREATE INDEX rel_socio_de_month IF NOT EXISTS FOR ()-[r:SOCIO_DE]-() ON (r.reference_month)",
        "CREATE INDEX rel_socio_de_entrada IF NOT EXISTS FOR ()-[r:SOCIO_DE]-() ON (r.data_entrada_sociedade)",
        "CREATE INDEX rel_socio_de_qualificacao IF NOT EXISTS FOR ()-[r:SOCIO_DE]-() ON (r.qualificacao_socio)",
    ]

    created_count = 0
    total_indexes = 0
    total_constraints = 0

    try:
        with driver.session() as session:
            for stmt in statements:
                try:
                    session.run(stmt)
                    # Extract index/constraint name from statement
                    name = stmt.split()[2]  # "CREATE INDEX|CONSTRAINT <name> IF..."
                    logger.info(f"  ✓ {name}")
                    created_count += 1
                except Exception as e:
                    # Log but don't fail — some indexes may already exist
                    logger.warning(f"  ⚠ {stmt[:50]}... → {e}")

            # Get final count of indexes/constraints (inside session scope)
            result = session.run("SHOW INDEXES YIELD name RETURN count(name) as total")
            total_indexes = result.single()["total"]

            result = session.run("SHOW CONSTRAINTS YIELD name RETURN count(name) as total")
            total_constraints = result.single()["total"]

        logger.info(f"✓ Schema ready: {total_constraints} constraints, {total_indexes} indexes")

    except Exception as e:
        logger.error(f"Failed to create indexes: {e}")
        raise
    finally:
        driver.close()
    
    return {
        "statements_executed": created_count,
        "total_indexes": total_indexes,
        "total_constraints": total_constraints
    }


@task
def check_neo4j_index_status(**context) -> dict:
    """
    Check the population status of Neo4j indexes.
    
    Returns:
        Dict with index population stats
    """
    from .config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
    
    logger.info("Checking Neo4j index status...")
    
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    try:
        with driver.session() as session:
            result = session.run("""
                SHOW INDEXES 
                YIELD name, state, populationPercent, type
                RETURN name, state, populationPercent, type
                ORDER BY name
            """)
            
            indexes = []
            populating = 0
            online = 0
            
            for record in result:
                index_info = {
                    "name": record["name"],
                    "state": record["state"],
                    "population_percent": record["populationPercent"],
                    "type": record["type"]
                }
                indexes.append(index_info)
                
                if record["state"] == "POPULATING":
                    populating += 1
                    logger.info(f"  ⏳ {record['name']}: {record['populationPercent']:.1f}%")
                elif record["state"] == "ONLINE":
                    online += 1
                    logger.info(f"  ✓ {record['name']}: ONLINE")
                else:
                    logger.warning(f"  ⚠ {record['name']}: {record['state']}")
            
            logger.info(f"Summary: {online} online, {populating} populating")
            
    except Exception as e:
        logger.error(f"Failed to check index status: {e}")
        raise
    finally:
        driver.close()
    
    return {
        "total_indexes": len(indexes),
        "online": online,
        "populating": populating,
        "indexes": indexes
    }
