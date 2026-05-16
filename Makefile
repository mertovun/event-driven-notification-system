.PHONY: help install-hooks fmt vet build test test-integration lint generate vuln tidy load-test load-baseline up down logs ps

COMPOSE := docker compose -f deploy/docker-compose.yml --env-file .env

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

test-integration: ## Run integration tests (testcontainers-go starts real services)
	@go test -race -count=1 -tags=integration ./...

lint: ## Run golangci-lint (requires it to be installed)
	@golangci-lint run

generate: ## Regenerate sqlc code
	@sqlc generate

vuln: ## Run govulncheck against the module
	@govulncheck ./...

load-test: ## Run all k6 load test scenarios against the running stack
	@k6 run loadtest/k6_baseline.js
	@k6 run loadtest/k6_priority.js
	@k6 run loadtest/k6_idempotency.js

load-baseline: ## Run k6 baseline scenario only
	@k6 run loadtest/k6_baseline.js

tidy: ## Run go mod tidy
	@go mod tidy

up: ## Start the full stack (postgres, rabbitmq, redis, app)
	@$(COMPOSE) up -d --build
	@echo "→ http://localhost:$${APP_HOST_PORT:-8090}/livez"

down: ## Stop the stack
	@$(COMPOSE) down

logs: ## Tail app logs
	@$(COMPOSE) logs -f app

ps: ## List running services
	@$(COMPOSE) ps
