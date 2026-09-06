COMPOSE := docker compose -f docker/docker-compose.yml
PY      := .venv/Scripts/python.exe
ifeq ($(wildcard $(PY)),)
PY      := .venv/bin/python
endif

# A copy of the stripe cli in .tools/ counts as installed, so the capture does not depend on a
# system-wide install. Falls back to whatever is on PATH.
STRIPE  := .tools/stripe.exe
ifeq ($(wildcard $(STRIPE)),)
STRIPE  := .tools/stripe
endif
ifeq ($(wildcard $(STRIPE)),)
STRIPE  := stripe
endif

.DEFAULT_GOAL := help

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install:  ## Create the host venv and install dev extras
	python -m venv .venv
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -e ".[dev]"
	@echo "ok. next: cp .env.example .env"

.PHONY: up
up:  ## Start kafka, minio and the iceberg rest catalog
	$(COMPOSE) up -d
	@echo "minio console: http://localhost:9001 (minioadmin / minioadmin)"

.PHONY: down
down:  ## Stop the stack, keeping volumes
	$(COMPOSE) down

.PHONY: clean
clean:  ## Stop the stack and drop its volumes
	$(COMPOSE) down -v

.PHONY: ps
ps:  ## Show container status
	$(COMPOSE) ps

.PHONY: logs
logs:  ## Follow stack logs
	$(COMPOSE) logs -f

.PHONY: login
login:  ## Authenticate the cli against a sandbox in the browser (or set STRIPE_API_KEY in .env)
	$(STRIPE) login

.PHONY: whoami
whoami:  ## Show which account and which mode the cli will act as
	$(PY) -m payment_ledger.stripe_cli whoami

.PHONY: capture
capture:  ## Run the webhook receiver; it starts `stripe listen` itself
	$(PY) -m payment_ledger.capture

.PHONY: trigger
trigger:  ## Make the sandbox emit the event types the ledger is built from
	@echo "triggering the charge lifecycle. disputes resolve asynchronously,"
	@echo "so leave the receiver running after this finishes."
	$(PY) -m payment_ledger.stripe_cli trigger

.PHONY: fixtures
fixtures:  ## Count what has been captured so far, by event type
	@find fixtures/events -name '*.json' -printf '%h\n' 2>/dev/null | sort | uniq -c | sort -rn || echo "nothing captured yet"

.PHONY: audit
audit:  ## Check the whole tree is safe to publish (no credentials, no account identifiers)
	$(PY) scripts/audit_publishable.py

.PHONY: hooks
hooks:  ## Install the pre-commit hook that refuses a commit carrying a credential
	cp scripts/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "installed. bypass a single commit with --no-verify if you ever need to."

.PHONY: check
check: audit lint test  ## Everything CI runs

.PHONY: test
test:  ## Run the tests
	$(PY) -m pytest

.PHONY: lint
lint:  ## Lint and format-check
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
