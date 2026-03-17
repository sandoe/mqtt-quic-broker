.PHONY: all build test bench lint run clean generate-certs docker docker-up

BINARY    := mqtt-quic-broker
BUILD_DIR := ./bin
CMD_DIR   := ./cmd/broker
VERSION   := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS   := -ldflags="-s -w -X main.version=$(VERSION)"

all: build

## build: Compile the broker binary
build:
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY) $(CMD_DIR)
	@echo "Built $(BUILD_DIR)/$(BINARY)"

## run: Run the broker with default config
run: build generate-certs
	$(BUILD_DIR)/$(BINARY) --config configs/broker.yaml

## test: Run all unit tests with race detector
test:
	go test -race -count=1 -timeout=120s ./...

## test-short: Run only fast unit tests
test-short:
	go test -short -count=1 ./...

## bench: Run benchmark tests
bench:
	go test -bench=. -benchmem -benchtime=3s ./internal/mqtt/... ./internal/broker/...

## lint: Run golangci-lint (install if missing)
lint:
	@which golangci-lint > /dev/null 2>&1 || \
		(curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.57.2)
	golangci-lint run ./...

## vet: Run go vet
vet:
	go vet ./...

## generate-certs: Generate self-signed TLS certificates for development
generate-certs:
	@bash scripts/generate_certs.sh certs

## docker: Build Docker image
docker:
	docker build -t mqtt-quic-broker:$(VERSION) .

## docker-up: Start broker and monitoring with Docker Compose
docker-up:
	docker-compose up -d broker

## docker-down: Stop Docker Compose services
docker-down:
	docker-compose down

## clean: Remove build artifacts and generated certs
clean:
	rm -rf $(BUILD_DIR) certs/*.crt certs/*.key certs/*.srl

## help: Show this help
help:
	@grep -E '^## [a-z]' $(MAKEFILE_LIST) | sed 's/## /  /'
