.PHONY: sqlc-gen run test clean

sqlc-gen:
	@echo "⚙️ Generating sqlc code..."
	@sqlc generate -f internal/store/sqlc.yaml

build: sqlc-gen
	@echo "🚀 Starting server..."
	@go build ./cmd/server

run: sqlc-gen
	@echo "🚀 Starting server..."
	@go run ./cmd/server

test:
	@go test -race -v ./internal/...

clean:
	rm -rf internal/store/*.go bin/


# .PHONY: init build run test lint docker-up sqlc-gen

# # 初始化依赖
# init:
# 	go mod tidy
# 	cd web && npm install

# # 生成类型安全的 SQLite 访问层（需先写 migrations/*.sql）
# sqlc-gen:
# 	sqlc generate

# # 编译
# build:
# 	go build -ldflags "-s -w" -o bin/sync-tool ./cmd/server

# # 本地运行（热重载推荐安装 air）
# run:
# 	go run ./cmd/server

# # 运行测试
# test:
# 	go test -race -coverprofile=coverage.out ./internal/...

# # 代码质量检查
# lint:
# 	golangci-lint run ./...

# # Docker 一键启动
# docker-up:
# 	docker compose -f configs/docker-compose.yml up -d