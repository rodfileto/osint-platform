"""
Configuration for CNPJ Ingestion Pipeline

Shared constants and paths used across all task modules.
"""

from pathlib import Path

# Environment configuration
BASE_PATH = Path("/opt/airflow/data/cnpj")
RAW_PATH = BASE_PATH / "raw"
STAGING_PATH = BASE_PATH / "staging"
PROCESSED_PATH = BASE_PATH / "processed"
TEMP_PATH = BASE_PATH / "temp"
TEMP_SSD_PATH = Path("/opt/airflow/temp_ssd")  # Mapped to fast local SSD

# Default month to process (can be overridden via DAG params)
DEFAULT_REFERENCE_MONTH = "2026-02"

# Neo4j connection settings (from environment variables)
import os
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "osint_graph_password")

# ---------------------------------------------------------------------------
# Campos enviados ao Neo4j — apenas os relevantes para traversal e identidade.
# Campos analíticos (capital_social), endereços completos e contatos ficam
# exclusivamente no PostgreSQL / MatView.
# ---------------------------------------------------------------------------

NEO4J_EMPRESA_FIELDS = {
    "cnpj_basico",           # chave de identidade / merge key
    "razao_social",          # nome da entidade
    "natureza_juridica",     # classificação jurídica (SA, LTDA, MEI…)
    "porte_empresa",         # MICRO, PEQUENA, MEDIA, GRANDE
    # excluídos: capital_social (analítico), qualificacao_responsavel (código interno),
    #            ente_federativo_responsavel (raramente preenchido)
}

NEO4J_ESTABELECIMENTO_FIELDS = {
    "cnpj_basico",               # FK para Empresa
    "cnpj_ordem",                # parte da PK do CNPJ
    "cnpj_dv",                   # parte da PK do CNPJ
    "identificador_matriz_filial",  # 1=Matriz, 2=Filial
    "nome_fantasia",             # nome comercial
    "situacao_cadastral",        # 2=Ativa, 3=Suspensa, 4=Inapta, 8=Baixada
    "data_inicio_atividade",     # data de abertura
    "cnae_fiscal_principal",     # setor de atividade
    "uf",                        # estado
    "municipio",                 # cidade
    # excluídos: endereço completo, contatos, datas/motivos de situação,
    #            cnae_fiscal_secundaria, codigo_municipio, codigo_pais, etc.
}
