-- Receita masks partner CPFs (***123456**), so a person is identified by the masked CPF plus the
-- name. Index that pair for person lookups and co-ownership joins; it also serves CPF-only
-- lookups, which makes the single-column index redundant.
CREATE INDEX IF NOT EXISTS idx_socio_cpf_nome ON cnpj.socio (cpf_cnpj_socio, nome_socio_razao_social);
DROP INDEX IF EXISTS cnpj.idx_socio_cpf_cnpj;
