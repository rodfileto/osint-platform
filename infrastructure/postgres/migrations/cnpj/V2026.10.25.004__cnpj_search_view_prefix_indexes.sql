-- Company search matches CNPJs by exact value or by prefix (cnpj_basico = / LIKE '123%', cnpj_14
-- LIKE '1234%'). The database collation is en_US, so a plain btree cannot serve LIKE prefixes; the
-- pattern_ops indexes serve both equality and prefix matches. Without them every search that
-- combines a CNPJ condition with the trigram name conditions scans the whole view.
CREATE INDEX IF NOT EXISTS idx_mv_cnpj_basico_pattern
    ON cnpj.mv_company_search (cnpj_basico varchar_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_mv_cnpj14_pattern
    ON cnpj.mv_company_search (cnpj_14 text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_mvinact_cnpj_basico_pattern
    ON cnpj.mv_company_search_inactive (cnpj_basico varchar_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_mvinact_cnpj14_pattern
    ON cnpj.mv_company_search_inactive (cnpj_14 text_pattern_ops);
