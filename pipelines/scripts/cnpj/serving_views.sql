-- Serving views recreated inside the gold publish swap transaction.
--
-- These views are bound to tables that are rebuilt on every publish (the swap
-- renames the tables, and views keep pointing at the pre-swap objects), so the
-- publish step re-runs this file inside the swap transaction. The definitions
-- here mirror the Flyway migrations:
--   cnpj.estabelecimento_completo  -> cnpj/V2__create_cnpj_schema.sql
--   geo.v_cnpj_br_municipality     -> geo/V2026.03.15.002__geo_create_br_reference_tables.sql
-- If either view changes in Flyway, change it here too.

CREATE OR REPLACE VIEW cnpj.estabelecimento_completo AS
SELECT
    e.*,
    (e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv) AS cnpj_completo,
    emp.razao_social,
    emp.capital_social,
    emp.porte_empresa
FROM cnpj.estabelecimento e
LEFT JOIN cnpj.empresa emp ON e.cnpj_basico = emp.cnpj_basico;

CREATE OR REPLACE VIEW geo.v_cnpj_br_municipality AS
SELECT
    cnpj_municipality.codigo                        AS cnpj_municipality_code,
    cnpj_municipality.nome                          AS cnpj_municipality_name,
    mapping.municipality_ibge_code                 AS municipality_ibge_code,
    municipality.municipality_name                 AS municipality_name,
    municipality.state_abbrev                      AS state_abbrev,
    municipality.country_code                      AS country_code,
    municipality.geobr_year                        AS geobr_year,
    municipality.is_simplified                     AS is_simplified,
    municipality.area_km2                          AS area_km2,
    municipality.centroid                          AS centroid,
    municipality.geom                              AS geom,
    mapping.match_method                           AS match_method,
    mapping.match_score                            AS match_score,
    mapping.mapping_source                         AS mapping_source,
    mapping.mapped_at                              AS mapped_at
FROM cnpj.municipio AS cnpj_municipality
LEFT JOIN geo.cnpj_br_municipality_map AS mapping
    ON mapping.cnpj_municipality_code = cnpj_municipality.codigo
LEFT JOIN geo.br_municipality AS municipality
    ON municipality.municipality_ibge_code = mapping.municipality_ibge_code;
