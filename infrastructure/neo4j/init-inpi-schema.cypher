// =====================================================
// Neo4j INPI Patents Graph Schema
//
// Nodes:
//   Patente         — one node per patent (codigo_interno PK from INPI)
//   ClasseIPC       — IPC classification symbol (e.g. B42F 11/02)
//   DepositantePF   — individual (pessoa física) filer, deduplicated by
//                     SHA-256(upper(nome) + "|" + cpf_mascara)
//
// Relationships:
//   DEPOSITOU       — (Empresa)->(Patente)  via cnpj_basico_resolved
//                     (DepositantePF)->(Patente)  via pessoa física records
//   CLASSIFICADA_COMO — (Patente)->(ClasseIPC)
//   VINCULADA_A     — (Patente)->(Patente)  derivation / incorporation
// =====================================================


// =====================================================
// Patente Node
// =====================================================

CREATE CONSTRAINT patente_codigo_interno IF NOT EXISTS
FOR (p:Patente) REQUIRE p.codigo_interno IS UNIQUE;

CREATE INDEX patente_numero_inpi IF NOT EXISTS
FOR (p:Patente) ON (p.numero_inpi);

CREATE INDEX patente_tipo IF NOT EXISTS
FOR (p:Patente) ON (p.tipo_patente);

CREATE INDEX patente_data_deposito IF NOT EXISTS
FOR (p:Patente) ON (p.data_deposito);

CREATE INDEX patente_data_publicacao IF NOT EXISTS
FOR (p:Patente) ON (p.data_publicacao);

CREATE INDEX patente_snapshot IF NOT EXISTS
FOR (p:Patente) ON (p.snapshot_date);

// Full-text search on patent title
CREATE FULLTEXT INDEX patente_titulo IF NOT EXISTS
FOR (p:Patente) ON EACH [p.titulo];


// =====================================================
// ClasseIPC Node
// =====================================================

CREATE CONSTRAINT ipc_simbolo IF NOT EXISTS
FOR (c:ClasseIPC) REQUIRE c.simbolo IS UNIQUE;

// grupo = LEFT(simbolo, 4) — e.g. "F41A" — indexed for prefix range queries
// (all defense patents: STARTS WITH 'F41', all aerospace: STARTS WITH 'B64')
CREATE INDEX ipc_grupo IF NOT EXISTS
FOR (c:ClasseIPC) ON (c.grupo);


// =====================================================
// DepositantePF Node
//   Pessoa física filer, deduplicated by SHA-256 of
//   upper(strip(nome)) + "|" + cpf_mascara (e.g. "****247****").
//   CPF mask is 3 exposed digits out of 11 → low entropy alone,
//   but combined with the normalized name provides a stable identity key.
// =====================================================

CREATE CONSTRAINT depositante_pf_id IF NOT EXISTS
FOR (d:DepositantePF) REQUIRE d.depositante_id IS UNIQUE;

CREATE INDEX depositante_pf_nome IF NOT EXISTS
FOR (d:DepositantePF) ON (d.nome);

CREATE FULLTEXT INDEX depositante_pf_nome_ft IF NOT EXISTS
FOR (d:DepositantePF) ON EACH [d.nome];


// =====================================================
// DEPOSITOU relationship  (Empresa)->(Patente) and (DepositantePF)->(Patente)
// =====================================================

CREATE INDEX rel_depositou_ordem IF NOT EXISTS
FOR ()-[r:DEPOSITOU]-() ON (r.ordem);


// =====================================================
// CLASSIFICADA_COMO relationship  (Patente)->(ClasseIPC)
// =====================================================

// No extra rel index needed — traversal via the node constraints is sufficient.


// =====================================================
// VINCULADA_A relationship  (Patente)->(Patente)
//   tipo_vinculo: A = Alteração, I = Incorporação
// =====================================================

CREATE INDEX rel_vinculada_tipo IF NOT EXISTS
FOR ()-[r:VINCULADA_A]-() ON (r.tipo_vinculo);


// =====================================================
// Example Queries
// =====================================================

// All patents filed by a company (by CNPJ basico)
// MATCH (e:Empresa {cnpj_basico: "33000167"})-[:DEPOSITOU]->(p:Patente)
// RETURN p.numero_inpi, p.titulo, p.tipo_patente, p.data_deposito
// ORDER BY p.data_deposito;

// Companies sharing the same IPC class (technology overlap / competitor analysis)
// MATCH (e:Empresa)-[:DEPOSITOU]->(p:Patente)-[:CLASSIFICADA_COMO]->(ipc:ClasseIPC)
// WHERE ipc.grupo = 'F41A'
// RETURN e.razao_social, COUNT(p) AS patents ORDER BY patents DESC LIMIT 20;

// Individual filer's full patent portfolio
// MATCH (d:DepositantePF)-[:DEPOSITOU]->(p:Patente)<-[:DEPOSITOU]-(e:Empresa)
// WHERE d.nome = 'HELLMUTH BRUCH'
// RETURN d.nome, p.numero_inpi, e.razao_social;

// Patent derivation chain
// MATCH path = (p:Patente)-[:VINCULADA_A*1..5]->(origem:Patente)
// WHERE p.numero_inpi = 'PI9500001'
// RETURN path;
