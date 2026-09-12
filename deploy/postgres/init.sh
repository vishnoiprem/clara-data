#!/bin/bash
# Create the Iceberg catalog's database alongside Clara's own.
# Postgres' official image runs every *.sh here once, on first initialisation.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
  CREATE DATABASE iceberg;
  GRANT ALL PRIVILEGES ON DATABASE iceberg TO $POSTGRES_USER;
SQL

echo "created iceberg database"
