.PHONY: help install-hooks fmt vet build test test-integration lint generate vuln tidy load-test load-baseline profile-cpu profile-heap profile-goroutine up down logs ps

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

profile-cpu: ## Capture a 30s CPU profile from the running app (requires PPROF_ENABLED=true)
	@mkdir -p loadtest/profiles
	@curl -sS "http://localhost:$${APP_HOST_PORT:-8090}/debug/pprof/profile?seconds=30" -o loadtest/profiles/cpu.pprof
	@echo "✓ loadtest/profiles/cpu.pprof — open with: go tool pprof -http=:6060 loadtest/profiles/cpu.pprof"

profile-heap: ## Snapshot the heap profile from the running app
	@mkdir -p loadtest/profiles
	@curl -sS "http://localhost:$${APP_HOST_PORT:-8090}/debug/pprof/heap" -o loadtest/profiles/heap.pprof
	@echo "✓ loadtest/profiles/heap.pprof"

profile-goroutine: ## Snapshot the goroutine profile
	@mkdir -p loadtest/profiles
	@curl -sS "http://localhost:$${APP_HOST_PORT:-8090}/debug/pprof/goroutine?debug=2" -o loadtest/profiles/goroutine.txt
	@echo "✓ loadtest/profiles/goroutine.txt"

tidy: ## Run go mod tidy
	@go mod tidy

up: ## Start the full stack (postgres, rabbitmq, redis, app)
	@BUILD_TIME=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		COMMIT=$$(git rev-parse --short=12 HEAD 2>/dev/null || echo local) \
		$(COMPOSE) up -d --build
	@echo "→ http://localhost:$${APP_HOST_PORT:-8090}/livez"

down: ## Stop the stack
	@$(COMPOSE) down

logs: ## Tail app logs
	@$(COMPOSE) logs -f app

ps: ## List running services
	@$(COMPOSE) ps
