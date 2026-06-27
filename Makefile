# Kafka REST API — developer tasks.
# Run `make help` for a list of targets.

BINARY      := kafka-rest-api
PKG         := github.com/JesusCabreraReveles/kafka-rest-api
CMD         := ./cmd/server
BIN_DIR     := bin
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -s -w -X main.version=$(VERSION)

GO          ?= go
GOLANGCI    := golangci-lint

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: tidy
tidy: ## Sync go.mod / go.sum.
	$(GO) mod tidy

.PHONY: build
build: ## Build the server binary into ./bin.
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$(BINARY) $(CMD)

.PHONY: run
run: ## Run the server locally.
	$(GO) run $(CMD)

.PHONY: test
test: ## Run all tests with the race detector.
	$(GO) test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and open an HTML coverage report.
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out

.PHONY: lint
lint: ## Run golangci-lint.
	$(GOLANGCI) run ./...

.PHONY: fmt
fmt: ## Format the codebase.
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

.PHONY: check
check: fmt vet lint test ## Run the full quality gate.

.PHONY: docker-build
docker-build: ## Build the Docker image.
	docker build -t $(BINARY):$(VERSION) -t $(BINARY):latest .

.PHONY: up
up: ## Start the full stack (app + kafka) via docker compose.
	docker compose up --build

.PHONY: down
down: ## Stop the stack and remove volumes.
	docker compose down -v

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BIN_DIR) coverage.out
