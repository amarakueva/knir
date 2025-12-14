DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset') THEN
        CREATE ROLE superset LOGIN PASSWORD 'superset';
    END IF;
END $$;

\connect postgres

SELECT 'CREATE DATABASE superset OWNER superset'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'superset')
\gexec
