-- A corporate partner's cpf_cnpj_socio is its full CNPJ; its first 8 digits are the partner
-- company's cnpj_basico. Index them so the ownership network can find the companies a company is a
-- partner in without scanning cnpj.socio (0.9-1.3 s per lookup before, under 1 ms after).
CREATE INDEX IF NOT EXISTS idx_socio_pj_cnpj_basico ON cnpj.socio (left(cpf_cnpj_socio, 8))
    WHERE identificador_socio = 1;
