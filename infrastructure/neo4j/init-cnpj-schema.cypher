// Neo4j CNPJ Graph Schema Initialization
// Creates indexes and constraints for CNPJ entities

// =====================================================
// Empresa Node (Company Base Data)
// =====================================================

// Constraint: Unique CNPJ básico
CREATE CONSTRAINT empresa_cnpj_basico IF NOT EXISTS
FOR (e:Empresa) REQUIRE e.cnpj_basico IS UNIQUE;

// Indexes for search performance
CREATE INDEX empresa_razao_social IF NOT EXISTS
FOR (e:Empresa) ON (e.razao_social);

CREATE INDEX empresa_porte IF NOT EXISTS
FOR (e:Empresa) ON (e.porte_empresa);

CREATE INDEX empresa_natureza IF NOT EXISTS
FOR (e:Empresa) ON (e.natureza_juridica);

// Full-text search on company names
CREATE FULLTEXT INDEX empresa_names IF NOT EXISTS
FOR (e:Empresa) ON EACH [e.razao_social];

// Indexes for timestamp-based queries
CREATE INDEX empresa_created_at IF NOT EXISTS
FOR (e:Empresa) ON (e.created_at);

CREATE INDEX empresa_updated_at IF NOT EXISTS
FOR (e:Empresa) ON (e.updated_at);


// =====================================================
// Estabelecimento Node (Establishment/Location)
// =====================================================

// Constraint: Unique complete CNPJ (14 digits)
CREATE CONSTRAINT estabelecimento_cnpj IF NOT EXISTS
FOR (est:Estabelecimento) REQUIRE est.cnpj IS UNIQUE;

// Indexes for geographic and status queries
CREATE INDEX estabelecimento_municipio IF NOT EXISTS
FOR (est:Estabelecimento) ON (est.municipio);

CREATE INDEX estabelecimento_uf IF NOT EXISTS
FOR (est:Estabelecimento) ON (est.uf);

CREATE INDEX estabelecimento_situacao IF NOT EXISTS
FOR (est:Estabelecimento) ON (est.situacao_cadastral);

CREATE INDEX estabelecimento_cnae IF NOT EXISTS
FOR (est:Estabelecimento) ON (est.cnae_fiscal_principal);

// Full-text search on trade names
CREATE FULLTEXT INDEX estabelecimento_names IF NOT EXISTS
FOR (est:Estabelecimento) ON EACH [est.nome_fantasia];

// Indexes for timestamp-based queries
CREATE INDEX estabelecimento_created_at IF NOT EXISTS
FOR (est:Estabelecimento) ON (est.created_at);

CREATE INDEX estabelecimento_updated_at IF NOT EXISTS
FOR (est:Estabelecimento) ON (est.updated_at);


// =====================================================
// Pessoa Node (Partner/Shareholder — Hybrid Model)
// =====================================================

// Constraint: Unique pessoa_id (hash key for deduplication)
CREATE CONSTRAINT pessoa_id IF NOT EXISTS
FOR (p:Pessoa) REQUIRE p.pessoa_id IS UNIQUE;

// Index on cpf_cnpj_socio for direct lookups (masked, but still useful)
CREATE INDEX pessoa_cpf_cnpj IF NOT EXISTS
FOR (p:Pessoa) ON (p.cpf_cnpj_socio);

// Index on identificador_socio for type filtering (1=PJ, 2=PF, 3=Estrangeiro)
CREATE INDEX pessoa_identificador IF NOT EXISTS
FOR (p:Pessoa) ON (p.identificador_socio);

// Index on faixa_etaria for OSINT age-band analysis
CREATE INDEX pessoa_faixa_etaria IF NOT EXISTS
FOR (p:Pessoa) ON (p.faixa_etaria);

// Full-text search on partner names
CREATE FULLTEXT INDEX pessoa_names IF NOT EXISTS
FOR (p:Pessoa) ON EACH [p.nome];


// =====================================================
// Relationships
// =====================================================

// Index on PERTENCE_A relationship for traversal performance
CREATE INDEX rel_pertence_a IF NOT EXISTS
FOR ()-[r:PERTENCE_A]-() ON (r.created_at);

// Composite index on SOCIO_DE for temporal and snapshot queries
CREATE INDEX rel_socio_de_month IF NOT EXISTS
FOR ()-[r:SOCIO_DE]-() ON (r.reference_month);

CREATE INDEX rel_socio_de_entrada IF NOT EXISTS
FOR ()-[r:SOCIO_DE]-() ON (r.data_entrada_sociedade);

CREATE INDEX rel_socio_de_qualificacao IF NOT EXISTS
FOR ()-[r:SOCIO_DE]-() ON (r.qualificacao_socio);


// =====================================================
// Example Queries (for reference)
// =====================================================

// Find company by CNPJ básico
// MATCH (e:Empresa {cnpj_basico: "12345678"}) RETURN e;

// Find all establishments of a company
// MATCH (est:Estabelecimento)-[:PERTENCE_A]->(e:Empresa {cnpj_basico: "12345678"})
// RETURN est;

// Find companies in a city
// MATCH (est:Estabelecimento {municipio: "SÃO PAULO"})-[:PERTENCE_A]->(e:Empresa)
// RETURN DISTINCT e, COUNT(est) as num_estabelecimentos
// ORDER BY num_estabelecimentos DESC;

// Full-text search for partners
// CALL db.index.fulltext.queryNodes("pessoa_names", "JOAO~")
// YIELD node, score
// RETURN node.pessoa_id, node.nome, node.identificador_socio, score
// ORDER BY score DESC LIMIT 10;

// OSINT: Partners with faixa_etaria >= 8 (60+ years) in 2+ hops from a target company
// MATCH (e:Empresa {cnpj_basico: "12345678"})<-[:SOCIO_DE*1..2]-(p:Pessoa)
// WHERE p.faixa_etaria >= 8
// RETURN p.nome, p.faixa_etaria, COUNT(*) as empresas_vinculadas
// ORDER BY empresas_vinculadas DESC;

// OSINT: PJ partners (holding structures)
// MATCH (p:Pessoa {identificador_socio: 1})-[r:SOCIO_DE]->(e:Empresa)
// WHERE r.reference_month = "2026-02"
// RETURN p.nome AS holding, e.razao_social AS empresa, r.qualificacao_socio
// LIMIT 100;

// Geographic distribution of establishments
// MATCH (est:Estabelecimento)
// RETURN est.uf as estado, COUNT(*) as total
// ORDER BY total DESC;
