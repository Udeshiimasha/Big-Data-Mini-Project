#!/bin/bash
set -e

# Create Airflow database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE airflowdb;
EOSQL

