// ============================================================================
// Script de Correção: Pessoa nodes incorretos para PJ socios
// ============================================================================
// 
// Problema: A versão anterior do código criou nós :Pessoa para sócios PJ
// (identificador_socio = 1), quando deveria ter criado relacionamentos
// diretos (:Empresa)-[:SOCIO_DE]->(:Empresa).
//
// Este script:
// 1. Identifica Pessoa nodes com identificador_socio = 1 (PJ)
// 2. Extrai o CNPJ básico (primeiros 8 dígitos) do cpf_cnpj_socio
// 3. Cria os relacionamentos corretos (:Empresa)-[:SOCIO_DE]->(:Empresa)
// 4. Remove os nós :Pessoa incorretos e suas relações
//
// Execução: 
//   docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password \
//     -f /plugins/fix-pj-pessoa-nodes.cypher
//
// ============================================================================

// Passo 1: Contar quantos nós :Pessoa incorretos existem
MATCH (p:Pessoa)
WHERE p.identificador_socio = 1
RETURN COUNT(p) AS incorrect_pessoa_nodes;

// Passo 2: Preview dos dados antes da conversão (primeiros 10)
MATCH (p:Pessoa)-[r:SOCIO_DE]->(e:Empresa)
WHERE p.identificador_socio = 1
RETURN 
    p.pessoa_id,
    p.cpf_cnpj_socio,
    p.nome,
    e.cnpj_basico,
    r.qualificacao_socio,
    r.data_entrada_sociedade,
    r.reference_month
LIMIT 10;

// Passo 3: Criar os relacionamentos corretos Empresa→Empresa
// Processar em batches de 10.000 para não sobrecarregar a transação
CALL apoc.periodic.iterate(
  "MATCH (p:Pessoa)-[r:SOCIO_DE]->(e_alvo:Empresa)
   WHERE p.identificador_socio = 1 
     AND p.cpf_cnpj_socio IS NOT NULL
     AND size(p.cpf_cnpj_socio) >= 8
   RETURN p, r, e_alvo",
  
  "WITH p, r, e_alvo, substring(p.cpf_cnpj_socio, 0, 8) AS cnpj_socio_basico
   MERGE (e_socio:Empresa {cnpj_basico: cnpj_socio_basico})
   ON CREATE SET e_socio.created_at = datetime(), e_socio._stub = true
   MERGE (e_socio)-[r_new:SOCIO_DE {reference_month: r.reference_month}]->(e_alvo)
   ON CREATE SET r_new.created_at = datetime()
   SET r_new.qualificacao_socio = r.qualificacao_socio,
       r_new.data_entrada_sociedade = r.data_entrada_sociedade,
       r_new.duplicate_count = r.duplicate_count,
       r_new.razao_social_socio = p.nome,
       r_new.updated_at = datetime(),
       r_new._migrated_from_pessoa = true
   WITH p, r
   DELETE r",
  
  {batchSize: 10000, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Passo 4: Remover os nós :Pessoa órfãos (sem relacionamentos)
// Apenas remove Pessoa com identificador_socio = 1 que não têm mais relações
CALL apoc.periodic.iterate(
  "MATCH (p:Pessoa)
   WHERE p.identificador_socio = 1 
     AND NOT (p)-[]-()
   RETURN p",
  
  "DELETE p",
  
  {batchSize: 10000, parallel: false}
)
YIELD batches, total, errorMessages
RETURN batches, total, errorMessages;

// Passo 5: Verificar resultado final
MATCH (p:Pessoa)
WHERE p.identificador_socio = 1
RETURN COUNT(p) AS remaining_incorrect_pessoa_nodes;

// Passo 6: Contar novos relacionamentos Empresa→Empresa criados
MATCH (e1:Empresa)-[r:SOCIO_DE]->(e2:Empresa)
WHERE r._migrated_from_pessoa = true
RETURN COUNT(r) AS empresa_empresa_relationships_created;

// Passo 7: Estatísticas finais
MATCH (e:Empresa)-[r:SOCIO_DE]->()
WITH 
  COUNT(CASE WHEN endNode(r):Empresa THEN 1 END) AS empresa_to_empresa,
  COUNT(CASE WHEN startNode(r):Pessoa THEN 1 END) AS pessoa_to_empresa,
  COUNT(r) AS total_socio_de
RETURN 
  empresa_to_empresa,
  pessoa_to_empresa,
  total_socio_de,
  empresa_to_empresa + pessoa_to_empresa AS verification_total;
