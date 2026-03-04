sudo systemctl restart docker-- Add missing timestamp columns to existing tables
-- This script is idempotent and safe to run multiple times

-- Add created_at and updated_at to empresa table if not exists
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'cnpj' 
        AND table_name = 'empresa' 
        AND column_name = 'created_at'
    ) THEN
        ALTER TABLE cnpj.empresa 
        ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        
        -- Set created_at for existing rows
        UPDATE cnpj.empresa 
        SET created_at = CURRENT_TIMESTAMP 
        WHERE created_at IS NULL;
        
        RAISE NOTICE 'Added created_at column to cnpj.empresa';
    ELSE
        RAISE NOTICE 'Column created_at already exists in cnpj.empresa';
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'cnpj' 
        AND table_name = 'empresa' 
        AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE cnpj.empresa 
        ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        
        -- Set updated_at for existing rows
        UPDATE cnpj.empresa 
        SET updated_at = CURRENT_TIMESTAMP 
        WHERE updated_at IS NULL;
        
        RAISE NOTICE 'Added updated_at column to cnpj.empresa';
    ELSE
        RAISE NOTICE 'Column updated_at already exists in cnpj.empresa';
    END IF;
END $$;

-- Add created_at and updated_at to estabelecimento table if not exists
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'cnpj' 
        AND table_name = 'estabelecimento' 
        AND column_name = 'created_at'
    ) THEN
        ALTER TABLE cnpj.estabelecimento 
        ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        
        -- Set created_at for existing rows
        UPDATE cnpj.estabelecimento 
        SET created_at = CURRENT_TIMESTAMP 
        WHERE created_at IS NULL;
        
        RAISE NOTICE 'Added created_at column to cnpj.estabelecimento';
    ELSE
        RAISE NOTICE 'Column created_at already exists in cnpj.estabelecimento';
    END IF;
    
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'cnpj' 
        AND table_name = 'estabelecimento' 
        AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE cnpj.estabelecimento 
        ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        
        -- Set updated_at for existing rows
        UPDATE cnpj.estabelecimento 
        SET updated_at = CURRENT_TIMESTAMP 
        WHERE updated_at IS NULL;
        
        RAISE NOTICE 'Added updated_at column to cnpj.estabelecimento';
    ELSE
        RAISE NOTICE 'Column updated_at already exists in cnpj.estabelecimento';
    END IF;
END $$;

-- Create trigger function to auto-update updated_at
CREATE OR REPLACE FUNCTION cnpj.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS update_empresa_updated_at ON cnpj.empresa;
DROP TRIGGER IF EXISTS update_estabelecimento_updated_at ON cnpj.estabelecimento;

-- Create triggers to auto-update updated_at on row updates
CREATE TRIGGER update_empresa_updated_at
    BEFORE UPDATE ON cnpj.empresa
    FOR EACH ROW
    EXECUTE FUNCTION cnpj.update_updated_at_column();

CREATE TRIGGER update_estabelecimento_updated_at
    BEFORE UPDATE ON cnpj.estabelecimento
    FOR EACH ROW
    EXECUTE FUNCTION cnpj.update_updated_at_column();

\echo '✓ Timestamp columns added successfully'
