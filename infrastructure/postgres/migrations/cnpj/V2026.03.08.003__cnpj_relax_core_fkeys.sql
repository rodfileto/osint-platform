-- ====================================================================
-- DROPPING CORE FOREIGN KEYS TO PREVENT BULK LOAD FAILURES
-- ====================================================================
-- Due to dirty data from Receita Federal (inconsistent database snapshots),
-- there are records in the auxiliary transactional tables (simples_nacional,
-- estabelecimento, socio) that point to a 'cnpj_basico' that is MISSING from 
-- the main 'empresa' file. Because the Airflow bulk-load inserts data in 
-- parallel or chunks, these orphans throw "Key is not present in table empresa".
--
-- We drop these constraints to allow the data to load. The application handles
-- missing relationships via OUTER JOINs dynamically.
-- ====================================================================

-- Drop FK that links simples_nacional to empresa
ALTER TABLE cnpj.simples_nacional DROP CONSTRAINT IF EXISTS simples_nacional_cnpj_basico_fkey;

-- Drop FK that links estabelecimento to empresa
ALTER TABLE cnpj.estabelecimento DROP CONSTRAINT IF EXISTS estabelecimento_cnpj_basico_fkey;

-- Drop FK that links socio to empresa
ALTER TABLE cnpj.socio DROP CONSTRAINT IF EXISTS socio_cnpj_basico_fkey;
