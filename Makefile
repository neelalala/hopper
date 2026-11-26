run:
	go run ./cmd/hopper/main.go

build: test
	go build -o bin/hopper ./cmd/hopper/main.go

test:
	go test -v -race --count=5 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	go vet ./...

clickhouse-up:
	docker compose up -d clickhouse

clickhouse-run-migration:
	docker exec clickhouse clickhouse-client --query "CREATE TABLE IF NOT EXISTS events (id UUID, name String, user_id String, client_time DateTime, server_time DateTime, ip String, user_agent String, user_os String, user_device String, properties JSON) ENGINE = MergeTree() ORDER BY (server_time, user_id);"

clickhouse-down:
	docker compose down clickhouse

clickhouse-clean:
	docker compose down -v clickhouse

.PHONY: run build test
