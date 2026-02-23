-- Quick fix: Add updated_at column
ALTER TABLE cnpj.empresa ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE cnpj.estabelecimento ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Verify
\echo 'Colunas timestamp:'
SELECT table_name, column_name FROM information_schema.columns 
WHERE table_schema='cnpj' 
  AND table_name IN ('empresa','estabelecimento') 
  AND column_name IN ('created_at','updated_at') 
ORDER BY table_name, column_name;
