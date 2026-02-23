-- Add PRIMARY KEY constraints to CNPJ tables

-- Add PRIMARY KEY to empresa
ALTER TABLE cnpj.empresa 
ADD PRIMARY KEY (cnpj_basico);

-- Add PRIMARY KEY to estabelecimento (composite key)
ALTER TABLE cnpj.estabelecimento 
DROP CONSTRAINT IF EXISTS estabelecimento_pkey;

ALTER TABLE cnpj.estabelecimento 
ADD PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);

-- Verify constraints
\echo 'Constraints added:'
SELECT 
    tc.table_name, 
    tc.constraint_name,
    tc.constraint_type,
    STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'cnpj' 
    AND tc.table_name IN ('empresa', 'estabelecimento')
    AND tc.constraint_type = 'PRIMARY KEY'
GROUP BY tc.table_name, tc.constraint_name, tc.constraint_type
ORDER BY tc.table_name;
