#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Validando las ocho consultas PostgreSQL..."
docker compose exec -T postgres \
    psql -v ON_ERROR_STOP=1 -U fraude -d fraude \
    < db/consultas/01_consultas_representativas.sql

echo "Validando las cinco consultas Cassandra..."
docker compose exec -T cassandra cqlsh \
    < nosql/cassandra/03_consultas.cql

echo "Validando las consultas Redis..."
docker compose exec -T redis redis-cli \
    < nosql/redis/02_consultas.redis

echo "Todas las consultas finalizaron sin errores."
