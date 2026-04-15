#!/bin/bash
set -e

echo "🐘 Starting local PostgreSQL for sync-tool testing..."

docker run -d --name sync-pg-test \
  -e POSTGRES_USER=sync_user \
  -e POSTGRES_PASSWORD=sync_pass \
  -e POSTGRES_DB=sync_test \
  -p 5432:5432 \
  postgres:15-alpine \
  -c max_connections=100

# 等待 PG 就绪
for i in {1..30}; do
  if pg_isready -h localhost -p 5432 -U sync_user > /dev/null 2>&1; then
    echo "✅ PostgreSQL ready"
    break
  fi
  echo "⏳ Waiting for PostgreSQL... ($i/30)"
  sleep 1
done

echo "🔑 Connection string for testing:"
echo "  host=localhost port=5432 user=sync_user password=sync_pass dbname=sync_test sslmode=disable"
echo ""
echo "🧪 Run integration test:"
echo "  RUN_PG_INTEGRATION=1 go test -v ./internal/connector -run TestPostgresConn_Integration"
echo ""
echo "🧹 Cleanup: docker rm -f sync-pg-test"