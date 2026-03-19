// =====================================================
// Neo4j FINEP NLP Graph Schema
//
// Nodes:
//   ProjetoFinep        — FINEP project, sourced from
//                         finep.projetos_operacao_direta +
//                         finep.nlp_document_topic
//
// Relationships:
//   PROPOSTO_POR        — (ProjetoFinep)->(Empresa)  via cnpj_proponente_norm
//   EXECUTADO_POR       — (ProjetoFinep)->(Empresa)  via cnpj_executor_norm
//   SIMILAR_TO          — (ProjetoFinep)->(ProjetoFinep) via k-NN NLP pipeline
// =====================================================


// =====================================================
// ProjetoFinep Node
// =====================================================

// Constraint: unique projeto id (PK from finep.projetos_operacao_direta)
CREATE CONSTRAINT projeto_finep_id IF NOT EXISTS
FOR (p:ProjetoFinep) REQUIRE p.id IS UNIQUE;

// Lookup indexes
CREATE INDEX projeto_finep_contrato IF NOT EXISTS
FOR (p:ProjetoFinep) ON (p.contrato);

CREATE INDEX projeto_finep_cnpj_proponente IF NOT EXISTS
FOR (p:ProjetoFinep) ON (p.cnpj_proponente_norm);

CREATE INDEX projeto_finep_cnpj_executor IF NOT EXISTS
FOR (p:ProjetoFinep) ON (p.cnpj_executor_norm);

CREATE INDEX projeto_finep_topic_id IF NOT EXISTS
FOR (p:ProjetoFinep) ON (p.topic_id);

CREATE INDEX projeto_finep_status IF NOT EXISTS
FOR (p:ProjetoFinep) ON (p.status);

CREATE INDEX projeto_finep_run_id IF NOT EXISTS
FOR (p:ProjetoFinep) ON (p.nlp_run_id);

// Full-text search on project title
CREATE FULLTEXT INDEX projeto_finep_titulo IF NOT EXISTS
FOR (p:ProjetoFinep) ON EACH [p.titulo];


// =====================================================
// PROPOSTO_POR relationship  (ProjetoFinep)->(Empresa)
//
// Source: finep.projetos_operacao_direta.cnpj_proponente_norm
// Links to existing Empresa nodes by cnpj_basico (first 8 digits)
// or by cnpj_completo if 14-digit CNPJ is stored.
// =====================================================

CREATE INDEX rel_proposto_por_run IF NOT EXISTS
FOR ()-[r:PROPOSTO_POR]-() ON (r.nlp_run_id);


// =====================================================
// EXECUTADO_POR relationship  (ProjetoFinep)->(Empresa)
//
// Source: finep.projetos_operacao_direta.cnpj_executor_norm
// NULL cnpj_executor_norm → relationship is not created.
// When cnpj_executor_norm = cnpj_proponente_norm both relationships
// point to the same Empresa node (two distinct rel types, same pair).
// =====================================================

CREATE INDEX rel_executado_por_run IF NOT EXISTS
FOR ()-[r:EXECUTADO_POR]-() ON (r.nlp_run_id);


// =====================================================
// SIMILAR_TO relationship  (ProjetoFinep)->(ProjetoFinep)
//
// Created by the NLP DAG load task.
// Properties: cosine_similarity (REAL), nlp_run_id (INT)
// Direction: smaller id → larger id (undirected semantics,
//   stored with consistent direction for deduplication).
// =====================================================

CREATE INDEX rel_similar_to_run IF NOT EXISTS
FOR ()-[r:SIMILAR_TO]-() ON (r.nlp_run_id);

CREATE INDEX rel_similar_to_cosine IF NOT EXISTS
FOR ()-[r:SIMILAR_TO]-() ON (r.cosine_similarity);


// =====================================================
// Example Queries (for reference)
// =====================================================

// Find all projects proposed by a company (by full CNPJ)
// MATCH (e:Empresa {cnpj_basico: "12345678"})<-[:PROPOSTO_POR]-(p:ProjetoFinep)
// RETURN p.titulo, p.status, p.topic_id;

// Find intermediary companies: execute projects they did not propose
// MATCH (p:ProjetoFinep)-[:EXECUTADO_POR]->(exec:Empresa)
// WHERE NOT (p)-[:PROPOSTO_POR]->(exec)
// MATCH (p)-[:PROPOSTO_POR]->(prop:Empresa)
// RETURN prop.razao_social, exec.razao_social, count(p) AS n_projetos
// ORDER BY n_projetos DESC;

// Find companies linked to projects similar to a given project
// MATCH (p:ProjetoFinep {id: 123})-[:SIMILAR_TO]-(p2:ProjetoFinep)
// MATCH (p2)-[:PROPOSTO_POR]->(e:Empresa)
// RETURN e.cnpj_basico, e.razao_social, count(p2) AS n_projetos_similares
// ORDER BY n_projetos_similares DESC;

// Cluster view: all projects in a topic with their companies
// MATCH (p:ProjetoFinep {topic_id: 5})-[:PROPOSTO_POR]->(e:Empresa)
// RETURN p.titulo, e.razao_social, p.status
// LIMIT 50;
