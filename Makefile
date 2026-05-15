.PHONY: help install-hooks fmt vet build test lint

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install-hooks: ## Install git hooks (run once after clone)
	@chmod +x scripts/pre-commit
	@ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
	@echo "✓ pre-commit hook installed: gofmt + go vet + go build"

fmt: ## Format Go sources
	@gofmt -w .

vet: ## Run go vet
	@go vet ./...

build: ## Build all packages
	@go build ./...

test: ## Run unit tests with race detector
	@go test -race -count=1 ./...

lint: ## Run golangci-lint (requires it to be installed)
	@golangci-lint run
