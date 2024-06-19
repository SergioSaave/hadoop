#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO
    \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'hive') THEN
            CREATE ROLE hive LOGIN PASSWORD 'hive';
        END IF;
    END
    \$\$;

    DO
    \$\$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'metastore') THEN
            PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE metastore');
        END IF;
    END
    \$\$;

    \c metastore

    GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
EOSQL
