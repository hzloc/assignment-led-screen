#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  SET TIME ZONE 'America/New_York';
  SELECT pg_reload_conf();
  CREATE EXTENSION pg_cron;
  \i '/init_sql/00-CREATE-RATES_TABLE.sql'
  \i '/init_sql/01-CREATE-MATERIALIZED-VIEWS.sql'
  \i '/init_sql/02-CRON-REFRESH-VIEW.sql'

EOSQL
