DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'stg') THEN
        EXECUTE 'CREATE SCHEMA stg';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'dm') THEN
        EXECUTE 'CREATE SCHEMA dm';
    END IF;
END $$;
