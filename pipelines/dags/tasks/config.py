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

# ---------------------------------------------------------------------------
# DEPRECATED (Cenário A) — Estabelecimento não é mais carregado no Neo4j.
# Filiais são consultadas via JOIN com cnpj.mv_company_search no PostgreSQL (SSD).
# Mantido aqui apenas como referência dos campos disponíveis no Parquet.
# ---------------------------------------------------------------------------
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
