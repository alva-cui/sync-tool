.PHONY: dev build test migrate sqlc lint docker

dev:
	@go run cmd/server/main.go

build:
	@CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/sync-tool cmd/server/main.go

test:
	@go test ./internal/... -race -coverprofile=coverage.out

lint:
	@golangci-lint run ./...

sqlc:
	@sqlc generate

migrate:
	@sqlite3 data/sync.db < internal/store/migrations/v1_init.sql

docker:
	@docker compose -f configs/docker-compose.yml up -d