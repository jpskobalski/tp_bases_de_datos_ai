#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose exec -T cassandra cqlsh < nosql/cassandra/01_schema.cql
docker compose exec -T cassandra cqlsh < nosql/cassandra/02_datos_sinteticos.cql
docker compose exec -T redis redis-cli < nosql/redis/01_datos_sinteticos.redis

echo "Redis y Cassandra fueron inicializados correctamente."
