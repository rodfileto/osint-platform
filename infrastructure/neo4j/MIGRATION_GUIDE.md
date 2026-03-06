# Correção de Sócios PJ no Neo4j

## Problema

A versão anterior do código de carga criou incorretamente **nós `:Pessoa`** para sócios Pessoa Jurídica (identificador_socio = 1), quando deveria ter criado **relacionamentos diretos** `(:Empresa)-[:SOCIO_DE]->(:Empresa)`.

### Impacto

- ~676.510 nós `:Pessoa` incorretos criados para empresas
- Estrutura do grafo incorreta para análise de cadeias de propriedade corporativa
- Impossibilidade de traçar ownership chains entre empresas

## Solução

Três opções para corrigir:

### Opção 1: Script Automático (Recomendado)

Usa APOC para processar em batches eficientemente.

```bash
./infrastructure/neo4j/fix-pj-socios.sh
```

**Requisitos:**
- Plugin APOC instalado no Neo4j
- Container `osint_neo4j` rodando

**O que faz:**
1. Verifica disponibilidade do APOC
2. Conta nós `:Pessoa` incorretos (identificador_socio = 1)
3. Solicita confirmação
4. Migra em batches de 10.000 registros:
   - Extrai CNPJ básico (8 primeiros dígitos) de `cpf_cnpj_socio`
   - Cria nó `:Empresa` para o sócio (ou faz MERGE se existir)
   - Cria relacionamento `(:Empresa)-[:SOCIO_DE]->(:Empresa)`
   - Remove relacionamento antigo `(:Pessoa)-[:SOCIO_DE]->(:Empresa)`
5. Remove nós `:Pessoa` órfãos (sem relacionamentos)
6. Exibe estatísticas finais

### Opção 2: Script Manual (Sem APOC)

Se APOC não estiver disponível, use o script simples:

```bash
docker exec -it osint_neo4j cypher-shell -u neo4j -p osint_graph_password
```

Depois copie e cole os blocos do arquivo:
```
infrastructure/neo4j/fix-pj-pessoa-nodes-simple.cypher
```

**Execute cada seção separadamente:**

1. **Diagnóstico** - verifica quantos nós precisam migração
2. **Migração** - processa 5.000 por vez (repetir até retornar 0)
3. **Limpeza** - remove nós órfãos (repetir até retornar 0)
4. **Verificação** - confirma sucesso da operação

### Opção 3: Limpar e Recarregar

Se preferir começar do zero com os sócios:

```bash
# 1. Remover todos os dados de sócios do Neo4j
docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password \
  "MATCH (p:Pessoa) DETACH DELETE p"

# 2. Reexecutar o DAG de carga (já com o código corrigido)
# Via Airflow UI: cnpj_load_neo4j com params:
#   entity_type: 'socios'
#   reference_month: '2026-02'
```

**Vantagem:** Dados limpos desde o início  
**Desvantagem:** Reprocessa todos os ~27M registros (~2-3 horas)

## Verificação Pós-Migração

Execute no cypher-shell:

```cypher
// Não deve ter Pessoa nodes com identificador_socio = 1
MATCH (p:Pessoa)
WHERE p.identificador_socio = 1
RETURN COUNT(p) AS incorrect_pessoa_nodes;
// Esperado: 0

// Verificar relacionamentos Empresa→Empresa criados
MATCH (e1:Empresa)-[r:SOCIO_DE]->(e2:Empresa)
RETURN COUNT(r) AS empresa_empresa_relationships;
// Esperado: ~676.510

// Estatísticas finais
MATCH (p:Pessoa) RETURN COUNT(p) AS total_pessoas;
// Esperado: ~17.5M (apenas PF/Estrangeiro)

MATCH (e:Empresa) RETURN COUNT(e) AS total_empresas;
// Esperado: ~66.7M

MATCH ()-[r:SOCIO_DE]->()
RETURN COUNT(r) AS total_socio_de;
// Esperado: ~27.1M (26.4M PF + 676K PJ)
```

## Arquivos

- `fix-pj-socios.sh` - Script shell automático (usa APOC)
- `fix-pj-pessoa-nodes.cypher` - Script Cypher com APOC
- `fix-pj-pessoa-nodes-simple.cypher` - Script Cypher sem APOC (manual)
- `MIGRATION_GUIDE.md` - Este arquivo

## Detalhes Técnicos

### Identificação de Nós Incorretos

```cypher
MATCH (p:Pessoa)
WHERE p.identificador_socio = 1  // PJ
  AND p.cpf_cnpj_socio IS NOT NULL
  AND size(p.cpf_cnpj_socio) >= 8
```

### Extração do CNPJ Básico

```cypher
substring(p.cpf_cnpj_socio, 0, 8) AS cnpj_socio_basico
```

O campo `cpf_cnpj_socio` para PJ contém o CNPJ completo (14 dígitos).
Os primeiros 8 dígitos formam o `cnpj_basico`, chave única da `:Empresa`.

### Preservação de Dados

Todos os atributos do relacionamento são preservados:
- `qualificacao_socio`
- `data_entrada_sociedade`
- `reference_month` (chave de MERGE)
- `duplicate_count`
- `razao_social_socio` (adicionado do campo `nome` da Pessoa)

### Marcação de Migração

Relacionamentos migrados recebem a propriedade:
```cypher
r._migrated = true  // ou r._migrated_from_pessoa = true
```

Isso permite auditoria e rollback se necessário.

## Tempo Estimado

- **Com APOC:** 5-10 minutos para ~676K registros
- **Sem APOC (manual):** 20-40 minutos (executar múltiplas vezes)
- **Reprocessamento completo:** 2-3 horas

## Suporte

Em caso de problemas, verificar logs do Neo4j:

```bash
docker logs osint_neo4j --tail 100
```

Ou entre no container para diagnóstico:

```bash
docker exec -it osint_neo4j bash
cd /logs
tail -f neo4j.log
```
