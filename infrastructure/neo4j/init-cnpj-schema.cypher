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
// Relationships
// =====================================================

// Index on PERTENCE_A relationship for traversal performance
CREATE INDEX rel_pertence_a IF NOT EXISTS
FOR ()-[r:PERTENCE_A]-() ON (r.created_at);


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

// Full-text search for companies
// CALL db.index.fulltext.queryNodes("empresa_names", "ACME~")
// YIELD node, score
// RETURN node.cnpj_basico, node.razao_social, score
// ORDER BY score DESC LIMIT 10;

// Geographic distribution
// MATCH (est:Estabelecimento)
// RETURN est.uf as estado, COUNT(*) as total
// ORDER BY total DESC;
