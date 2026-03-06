// ============================================================================
// Script de Correção Simples: Pessoa nodes PJ → Empresa relationships
// ============================================================================
// 
// Versão SEM APOC - usa UNWIND com LIMIT para processar em batches
//
// Este script corrige Pessoa nodes com identificador_socio = 1 (PJ),
// criando os relacionamentos corretos Empresa→Empresa.
//
// Execução manual (executar cada bloco separadamente):
//   docker exec osint_neo4j cypher-shell -u neo4j -p osint_graph_password
//
// ============================================================================

// === PASSO 1: Diagnóstico ===
// Contar quantos nós Pessoa incorretos existem
MATCH (p:Pessoa)
WHERE p.identificador_socio = 1
RETURN COUNT(p) AS total_pj_pessoa_nodes;

// Verificar amostra dos dados
MATCH (p:Pessoa)-[r:SOCIO_DE]->(e:Empresa)
WHERE p.identificador_socio = 1
RETURN 
    p.cpf_cnpj_socio,
    substring(p.cpf_cnpj_socio, 0, 8) AS cnpj_basico_extraido,
    p.nome,
    e.cnpj_basico AS empresa_alvo,
    r.reference_month
LIMIT 5;


// === PASSO 2: Criar Relacionamentos Corretos ===
// EXECUTAR ESTE BLOCO MÚLTIPLAS VEZES ATÉ RETORNAR 0
// Processa 5.000 registros por vez para não sobrecarregar

MATCH (p:Pessoa)-[r:SOCIO_DE]->(e_alvo:Empresa)
WHERE p.identificador_socio = 1 
  AND p.cpf_cnpj_socio IS NOT NULL
  AND size(p.cpf_cnpj_socio) >= 8
WITH p, r, e_alvo, substring(p.cpf_cnpj_socio, 0, 8) AS cnpj_socio_basico
LIMIT 5000

MERGE (e_socio:Empresa {cnpj_basico: cnpj_socio_basico})
ON CREATE SET e_socio.created_at = datetime(), e_socio._stub = true

MERGE (e_socio)-[r_new:SOCIO_DE {reference_month: r.reference_month}]->(e_alvo)
ON CREATE SET r_new.created_at = datetime()
SET r_new.qualificacao_socio = r.qualificacao_socio,
    r_new.data_entrada_sociedade = r.data_entrada_sociedade,
    r_new.duplicate_count = r.duplicate_count,
    r_new.razao_social_socio = p.nome,
    r_new.updated_at = datetime(),
    r_new._migrated = true

WITH r
DELETE r

RETURN COUNT(*) AS relationships_migrated;


// === PASSO 3: Remover Nós Pessoa Órfãos ===
// EXECUTAR ESTE BLOCO MÚLTIPLAS VEZES ATÉ RETORNAR 0
// Remove nós :Pessoa com identificador = 1 que não têm mais relacionamentos

MATCH (p:Pessoa)
WHERE p.identificador_socio = 1 
  AND NOT (p)-[]-()
WITH p LIMIT 5000
DELETE p
RETURN COUNT(*) AS pessoa_nodes_deleted;


// === PASSO 4: Verificação Final ===

// Contar Pessoa nodes PJ remanescentes (deve ser 0)
MATCH (p:Pessoa)
WHERE p.identificador_socio = 1
RETURN COUNT(p) AS remaining_pj_pessoa_nodes;

// Contar novos relacionamentos Empresa→Empresa
MATCH (e1:Empresa)-[r:SOCIO_DE]->(e2:Empresa)
WHERE r._migrated = true
RETURN COUNT(r) AS migrated_empresa_relationships;

// Estatísticas finais do grafo
CALL {
  MATCH ()-[r:SOCIO_DE]->()
  WHERE startNode(r):Empresa AND endNode(r):Empresa
  RETURN COUNT(r) AS empresa_to_empresa
}
CALL {
  MATCH ()-[r:SOCIO_DE]->()
  WHERE startNode(r):Pessoa
  RETURN COUNT(r) AS pessoa_to_empresa
}
RETURN empresa_to_empresa, pessoa_to_empresa, 
       empresa_to_empresa + pessoa_to_empresa AS total_socio_de;
