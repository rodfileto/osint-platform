-- Reset PostgreSQL CNPJ schema
-- Drop and recreate everything from scratch

\echo 'Dropping CNPJ schema...'
DROP SCHEMA IF EXISTS cnpj CASCADE;

\echo 'Creating CNPJ schema...'
CREATE SCHEMA IF NOT EXISTS cnpj;

\echo '✓ PostgreSQL CNPJ schema reset complete'
\echo 'Run init-cnpj-schema.sql to recreate tables'
