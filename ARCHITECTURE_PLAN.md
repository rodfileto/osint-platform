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

Cada fonte de dados tem seu próprio pipeline independente:

- Localização: `/pipelines/{fonte}/`
- Ferramentas: **DuckDB** (transformações bulk), **Python**, **Apache Airflow** (orquestração)
- DAGs encadeadas via `TriggerDagRunOperator` — cada DAG aciona a próxima ao terminar
- Outputs: tabelas limpas no PostgreSQL, nós/arestas no Neo4j, MinIO como raw store

**Pipelines implementados:**

| Fonte | Fluxo | Cadência |
|-------|-------|----------|
| CNPJ (Receita Federal) | `download → transform (Parquet) → load_postgres → load_neo4j` | Mensal (full snapshot) |
| FINEP (projetos de inovação) | `download → load_postgres → nlp_topics → load_neo4j` | Sob demanda |
| INPI (patentes) | `download → load_postgres → load_neo4j` | Semanal (detecção via ETag/Last-Modified) |

---

## 2. Estratégia de Storage

### PostgreSQL — Camada Relacional

Cluster único com separação por schema:

| Schema | Conteúdo | Status |
|--------|----------|--------|
| `cnpj.*` | Empresas, estabelecimentos, sócios, manifests de pipeline | ✅ Implementado |
| `finep.*` | Projetos de inovação, liberações, tópicos NLP | ✅ Implementado |
| `inpi.*` | 10 tabelas de patentes + `mv_patent_search` (145k patentes) | ✅ Implementado |
| `sanctions.*` | Entidades sancionadas | Planejado |
| `contracts.*` | Contratos públicos | Planejado |
| `public.*` | Auth compartilhada | — |

**Schema `inpi.*` — tabelas implementadas:**  
`patentes_dados_bibliograficos` (tabela pai, 145k) · `patentes_conteudo` · `patentes_inventores` · `patentes_depositantes` · `patentes_classificacao_ipc` · `patentes_despachos` · `patentes_prioridades` · `patentes_procuradores` · `patentes_vinculos` · `patentes_renumeracoes`  
Materialized view: `mv_patent_search` (PI=83.7k / MU=50.5k / MI=9.5k / PP=1.2k)  
Cross-domain: `patentes_depositantes.cnpj_basico_resolved` — CNPJ de pessoa jurídica resolvido no load para JOIN com `cnpj.empresa` e para arestas Neo4j.

**Desempenho:** dados históricos (HDD) + Materialized Views de busca (SSD via Tablespace).
Isso permite que a camada de busca do frontend use I/O de SSD sem pagar o custo do HDD para dados raw.

**Layout atual de volumes:**
- Data dir principal: `/srv/osint/postgres/data` (NVMe)
- Tablespace de busca `fast_ssd`: `/srv/osint/postgres/ssd_tablespace` (NVMe)
- Estratégia: PostgreSQL permanece no NVMe para menor latência; datasets de arquivo e objeto ficam fora do cluster, em HDD

### Neo4j — Camada de Grafo

Grafo unificado entre todas as fontes para inteligência cross-domínio.

**Nodes implementados:**

| Label | Chave | Contagem (prod) | Fonte |
|-------|-------|-----------------|-------|
| `Empresa` | `cnpj_basico` | 66.6M | CNPJ |
| `Pessoa` | `pessoa_id` (SHA-256) | 17.5M+ | CNPJ sócios |
| `ProjetoFinep` | `id` | ~25k | FINEP operação direta |
| `Patente` | `codigo_interno` | 145k | INPI |
| `ClasseIPC` | `simbolo` | 36.3k | INPI |
| `DepositantePF` | `depositante_id` (SHA-256) | 13.5k | INPI |

**Relationships implementados:**

| Tipo | De → Para | Fonte |
|------|-----------|-------|
| `SOCIO_DE` | `Pessoa → Empresa` | CNPJ |
| `PROPOSTO_POR` | `ProjetoFinep → Empresa` | FINEP |
| `EXECUTADO_POR` | `ProjetoFinep → Empresa` | FINEP |
| `SIMILAR_TO` | `ProjetoFinep → ProjetoFinep` | FINEP NLP (k-NN cosine) |
| `DEPOSITOU` | `Empresa → Patente` | INPI (via `cnpj_basico_resolved`) |
| `DEPOSITOU` | `DepositantePF → Patente` | INPI (pessoa física) |
| `CLASSIFICADA_COMO` | `Patente → ClasseIPC` | INPI |
| `VINCULADA_A` | `Patente → Patente` | INPI (A=Alteração, I=Incorporação) |

**Nodes planejados:** `Contrato`, `SanctionedEntity`  
**Relationships planejados:** `CONTRATOU`, `DOADOR_DE`, `LOCALIZADO_EM`

**Deduplicação de identidade:**  
`Pessoa` e `DepositantePF` usam SHA-256 de campos normalizados como chave estável — permite MERGE idempotente e consolida múltiplas aparições de uma mesma pessoa em um único nó.  
Linkage cross-domínio: `Empresa.cnpj_basico` é a chave de junção universal entre CNPJ, FINEP e INPI.

Permite consultas como: *"Quais empresas depositaram patentes em defesa (IPC F41\*) e também aparecem em projetos FINEP de mesmo tema?"*

**Layout atual de volumes:**
- Data dir Neo4j: `/srv/osint/neo4j/data` (NVMe)
- Plugins: `./infrastructure/neo4j/plugins`

### Storage Tiers

```
NVMe (/srv/osint)           →  PostgreSQL, tablespace `fast_ssd`, Neo4j, temp rápido do Airflow
HDD 10 TB (/mnt/data10tb)   →  `/opt/airflow/data` (raw/staging/processed) + MinIO object storage
HDD 2 TB (/mnt/data2tb)     →  backups e retenção operacional
```

**Mounts nativos do host:**
- `/mnt/data10tb` — volume bulk para pipelines e MinIO
- `/mnt/data2tb` — volume de backup/retenção
- `/srv/osint` — diretórios quentes de serviço no NVMe

**Object storage (MinIO):**
- Serviço local para arquivos raw/processados, logs arquivados e backups
- Data dir: `/mnt/data10tb/osint/minio`
- Buckets iniciais planejados: `osint-raw`, `osint-processed`, `osint-airflow-logs`, `osint-backups`
- No pipeline CNPJ, os ZIPs raw são tratados como source of truth em `osint-raw/cnpj/raw/<YYYY-MM>/...`
- A DAG `cnpj_download` faz streaming direto de Receita Federal para MinIO; o host local não é mais uma camada durável de raw ZIPs
- A DAG `cnpj_transform` baixa sob demanda do MinIO apenas como etapa temporária de extração para staging

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
| Object storage | MinIO | Arquivos raw/processados, logs e backups |
| Cache / fila | Redis | Sessões, filas Airflow |
| Workers assíncronos | Celery | Background tasks (planejado) |

### Convenções Operacionais

- Paths internos dos containers permanecem estáveis (`/opt/airflow/data`, `/opt/airflow/temp_ssd`, `/var/lib/postgresql/data`, `/data` no Neo4j)
- O mapeamento para discos físicos é feito por bind mounts no `docker-compose.yml`
- O ambiente usa mounts nativos em `/mnt` em vez de automounts em `/media`
- O automount do GNOME foi desabilitado para evitar remount automático dos HDDs fora do `fstab`

---

*Para configuração de hardware, tuning de banco e histórico de implantação, veja [IMPLEMENTATION_STEPS.md](IMPLEMENTATION_STEPS.md).*
