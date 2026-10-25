"""Paths and Neo4j settings shared by the CNPJ pipeline task modules."""

import os
from pathlib import Path

# Airflow temp volume (fast local disk): CSV staging, Parquet output, DuckDB spill files.
TEMP_SSD_PATH = Path("/opt/airflow/temp_ssd")
STAGING_PATH = TEMP_SSD_PATH / "cnpj_staging"
PROCESSED_PATH = TEMP_SSD_PATH / "cnpj_processed"

# Neo4j connection settings (from environment variables)
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

# ---------------------------------------------------------------------------
# Modelo Híbrido — Sócios (Pessoa + relacionamento SOCIO_DE)
#
# Nó :Pessoa — apenas campos essenciais para traversal e identidade (5 props).
# Campos de detalhe (pais, representante_legal, qualificacao_representante_legal,
# created_at, updated_at) ficam exclusivamente no PostgreSQL.
# ---------------------------------------------------------------------------

NEO4J_PESSOA_NODE_FIELDS = {
    "nome",                 # nome_socio_razao_social — busca textual e identidade
    "cpf_cnpj_socio",       # CPF/CNPJ mascarado — join com Postgres quando necessário
    "identificador_socio",  # 1=PJ, 2=PF, 3=Estrangeiro — filtro frequente no grafo
    "faixa_etaria",         # alta utilidade OSINT (análise de rede por faixa etária)
    # excluídos do nó: pais (lookup pontual), representante_legal / nome_do_representante
    #   / qualificacao_representante_legal (dados de detalhe, não de travessia),
    #   created_at / updated_at (controle operacional)
}

# Campos do relacionamento [:SOCIO_DE] — (Pessoa)-[:SOCIO_DE]->(Empresa)
NEO4J_SOCIO_REL_FIELDS = {
    "qualificacao_socio",        # tipo de vínculo (sócio, administrador, etc.)
    "data_entrada_sociedade",    # análise temporal de vínculos
    # reference_month e duplicate_count são adicionados em Python, não vêm do parquet
}
