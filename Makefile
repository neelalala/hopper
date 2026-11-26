run:
	go run ./cmd/hopper/main.go

build: test
	go build -o bin/hopper ./cmd/hopper/main.go

test:
	go test -v -race --count=5 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	go vet ./...

.PHONY: run build test