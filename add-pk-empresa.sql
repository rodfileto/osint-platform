-- Step 1: Check for duplicates before adding constraint
SELECT 'Checking for duplicate cnpj_basico in empresa...' as status;
SELECT cnpj_basico, COUNT(*) as count 
FROM cnpj.empresa 
GROUP BY cnpj_basico 
HAVING COUNT(*) > 1 
LIMIT 10;

-- Step 2: If no duplicates or after cleaning, add PRIMARY KEY
-- This might take a while on large tables
SELECT 'Adding PRIMARY KEY to empresa (this may take a few minutes)...' as status;
ALTER TABLE cnpj.empresa ADD PRIMARY KEY (cnpj_basico);

SELECT 'Done! Verifying...' as status;
SELECT conname, contype FROM pg_constraint 
WHERE conrelid = 'cnpj.empresa'::regclass AND contype = 'p';
