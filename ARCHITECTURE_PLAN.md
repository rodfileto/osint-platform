# OSINT Platform Architecture Plan

> Documento de referência de **decisões arquiteturais e stack tecnológico**.
> Para configuração de ambiente, hardware e histórico de execução, veja [IMPLEMENTATION_STEPS.md](IMPLEMENTATION_STEPS.md).

---

## Princípio Central

**Separar em camadas independentes:**

| Camada | Responsabilidade | Localização |
|--------|-----------------|-------------|
| Pipelines | Processamento e ingestão de dados | `/pipelines` |
| Storage | Persistência estruturada e grafo | PostgreSQL + Neo4j |
| API | Gateway stateless para a UI | `/backend` |
| Frontend | Apresentação e busca | `/frontend` |

**Regra:** Django não processa dados. Airflow não serve HTTP. Pipelines não têm dependência do Django.

---

## 1. Camada de Processamento (Pipelines)

Cada fonte de dados (CNPJ, Sanções, Contratos) tem seu próprio pipeline independente:

- Localização: `/pipelines/{fonte}/`
- Ferramentas: **DuckDB** (transformações bulk), **Python**, **Apache Airflow** (orquestração)
- Fluxo padrão: `Download → Transform (Parquet) → Load PostgreSQL → MatView Refresh → Load Neo4j`
- DAGs encadeadas via `TriggerDagRunOperator` — cada DAG aciona a próxima ao terminar
- Outputs: tabelas limpas no PostgreSQL, nós/arestas no Neo4j, Parquet como intermediário
- Dumps mensais são **full snapshots** — apenas o mês mais recente é processado

---

## 2. Estratégia de Storage

### PostgreSQL — Camada Relacional

Cluster único com separação por schema:

- `cnpj.*` — empresas, estabelecimentos, manifests de pipeline
- `sanctions.*` — entidades sancionadas
- `contracts.*` — contratos públicos
- `public.*` — auth compartilhada

**Desempenho:** dados históricos (HDD) + Materialized Views de busca (SSD via Tablespace).
Isso permite que a camada de busca do frontend use I/O de SSD sem pagar o custo do HDD para dados raw.

### Neo4j — Camada de Grafo

Grafo unificado entre todas as fontes para inteligência cross-domínio:

- **Nodes (implementados):** `Empresa` (66.6M), `Estabelecimento` (69.1M)
- **Nodes (planejados):** `Pessoa`, `Contrato`, `SanctionedEntity`
- **Relationships (planejados):** `SOCIO_DE`, `CONTRATOU`, `DOADOR_DE`, `LOCALIZADO_EM`

Permite consultas como: *"Um sócio da Empresa A aparece em lista de sanções e venceu licitações públicas?"*

### Storage Tiers

```
HDD (/media/bigdata)  →  tabelas raw (cnpj.empresa, cnpj.estabelecimento)
SSD (/)               →  Materialized Views + índices de busca (fast_ssd tablespace)
```

---

## 3. Django API

**Papel:** gateway stateless e orquestrador de consultas.

- Localização: `/backend`
- Apps: `cnpj`, `sanctions`, `contracts`, `core`
- Django consome os bancos produzidos pelos pipelines — não tem lógica de ETL

---

## 4. Frontend (Next.js)

**Papel:** dashboard de inteligência modular.

- Localização: `/frontend`
- Barra de busca unificada (CNPJ, CPF, nome)
- Visualizador de grafo
- Módulos por domínio: `/modules/cnpj`, `/modules/sanctions`

---

## 5. Entity Resolution

Camada de resolução de identidade global — o diferencial OSINT:

- Um `Person` node por pessoa real (chave: CPF)
- Um `Company` node por empresa real (chave: CNPJ raiz)
- Todos os datasets se ligam a esses nodes centrais

Permite descoberta de conexões entre domínios diferentes.

---

## 6. Stack de Infraestrutura

| Componente | Ferramenta | Papel |
|------------|-----------|-------|
| Containers | Docker Compose | Orquestra todos os serviços |
| ETL | Apache Airflow 2.x | Agenda e monitora pipelines |
| Storage relacional | PostgreSQL 16 | Dados estruturados + busca |
| Storage grafo | Neo4j | Inteligência de relacionamentos |
| Cache / fila | Redis | Sessões, filas Airflow |
| Workers assíncronos | Celery | Background tasks (planejado) |

---

*Para configuração de hardware, tuning de banco e histórico de implantação, veja [IMPLEMENTATION_STEPS.md](IMPLEMENTATION_STEPS.md).*
