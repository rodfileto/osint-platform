-- Receita Federal publishes a handful of placeholder companies with an empty name,
-- natureza_juridica 0000 and qualificacao 00 (6 of 70.5M in 2026-10). Keep them so
-- establishments and partners that reference them still resolve.
ALTER TABLE cnpj.empresa ALTER COLUMN razao_social DROP NOT NULL;
