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

# A generated run is fully described by these three. Override on the command line:
#   make e2e N=100000 SEED=7 DAYS=90
N     ?= 1000
SEED  ?= 1
DAYS  ?= 30

# Named here rather than inline so the reset recipe stays one readable line per thing it drops.
DROP_TABLES := \
  DROP TABLE IF EXISTS lakehouse.bronze.events; \
  DROP TABLE IF EXISTS lakehouse.silver.events; \
  DROP TABLE IF EXISTS lakehouse.silver.charges; \
  DROP TABLE IF EXISTS lakehouse.silver.refunds; \
  DROP TABLE IF EXISTS lakehouse.silver.disputes; \
  DROP TABLE IF EXISTS lakehouse.silver.balance_transactions; \
  DROP TABLE IF EXISTS lakehouse.silver.balance_transaction_list; \
  DROP TABLE IF EXISTS lakehouse.gold.ledger_postings; \
  DROP TABLE IF EXISTS lakehouse.gold.account_balances; \
  DROP TABLE IF EXISTS lakehouse.gold.daily_close; \
  DROP TABLE IF EXISTS lakehouse.gold.reconciliation; \
  DROP TABLE IF EXISTS lakehouse.gold.reconciliation_items; \
  DROP TABLE IF EXISTS lakehouse.gold.coverage_gaps; \
  DROP TABLE IF EXISTS lakehouse.gold.money_flow; \
  DROP TABLE IF EXISTS lakehouse.gold.daily_close_log; \
  DROP TABLE IF EXISTS lakehouse.gold.restatements; \
  DROP TABLE IF EXISTS lakehouse.silver.reported_balance;

.DEFAULT_GOAL := help

.PHONY: help
help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install:  ## Create the host venv and install dev extras
	python -m venv .venv
	$(PY) -m pip install --quiet --upgrade pip
	$(PY) -m pip install --quiet -e ".[dev,pipeline]"
	@echo "ok. next: cp .env.example .env"

.PHONY: up
up:  ## Start kafka, minio, the iceberg rest catalog and the thrift server dbt talks to
	$(COMPOSE) up -d
	$(COMPOSE) --profile jobs up -d spark-thrift
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

.PHONY: dispute
dispute:  ## Drive one inquiry all the way to a settled chargeback (the trigger only makes inquiries)
	$(PY) -m payment_ledger.stripe_cli dispute-lost

.PHONY: produce
produce:  ## Publish the captured fixtures to kafka (needs no stripe account)
	$(PY) -m payment_ledger.producer

.PHONY: bronze
bronze:  ## Drain the topic into bronze.events; spark runs in a container, the host needs no jvm
	$(COMPOSE) run --rm spark /opt/payment-ledger/jobs/bronze_events.py

.PHONY: generate
generate:  ## Simulate N transactions from the captured shapes (N=1000 SEED=1 DAYS=30)
	$(PY) -m payment_ledger.generator -n $(N) --seed $(SEED) --days $(DAYS)

.PHONY: e2e
e2e: reset generate  ## Generate N transactions and take them all the way to what a dashboard reads
	$(PY) -m payment_ledger.producer --events data/generated/events.jsonl
	$(MAKE) bronze
	$(MAKE) silver
	$(MAKE) gold
	$(MAKE) export

.PHONY: silver
silver:  ## Deduplicate bronze into silver.events, then project it onto one row per entity
	$(COMPOSE) run --rm spark /opt/payment-ledger/jobs/silver_events.py
	$(COMPOSE) run --rm spark /opt/payment-ledger/jobs/silver_entities.py
	$(COMPOSE) run --rm spark /opt/payment-ledger/jobs/balance_transaction_list.py
	$(COMPOSE) run --rm spark /opt/payment-ledger/jobs/reported_balance.py

.PHONY: gold
gold:  ## Build the double-entry ledger and run the invariants; a failing one fails the build
	$(COMPOSE) run --rm dbt build

# Four pieces of state, and forgetting any one of them makes the next run lie: the topic still
# holds the last run's events, the checkpoint still says they were consumed, the catalog still
# lists the table, and the bucket still holds its files. `DROP TABLE ... PURGE` is deliberately
# not used: against MinIO it logs a failure per object and leaves them behind, so the files are
# removed where they actually live.
.PHONY: reset
reset:  ## Start over: drop the topic, the checkpoints, every table and their files
	-$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka:19092 --delete --topic stripe.events.raw
	$(COMPOSE) up -d --force-recreate kafka-init
	$(COMPOSE) run --rm --entrypoint /bin/sh spark \
	  -c "rm -rf /opt/payment-ledger/checkpoints/*"
	$(COMPOSE) run --rm --entrypoint /opt/spark/bin/spark-sql spark -e "$(DROP_TABLES)"
	$(COMPOSE) run --rm --entrypoint /bin/sh minio-init \
	  -c "mc alias set l http://minio:9000 minioadmin minioadmin >/dev/null && \
	      mc rm --recursive --force --quiet \
	        l/warehouse/bronze l/warehouse/silver l/warehouse/gold || true"

# --- phase 5: the chaos suite --------------------------------------------------------------------
# The suite itself is a module rather than a wall of recipe, for a reason worth stating: the steps
# it submits can then be read by a test. Every table an arm touches has to carry that arm's name,
# and getting that wrong turns a chaos suite into six runs of the same undamaged pipeline nodding
# at each other. That is checked in `tests/test_chaos_run.py`, not hoped for here.

# --- phase 6: what a dashboard reads --------------------------------------------------------------

.PHONY: export
export:  ## Dump the gold tables and assemble the artifacts a dashboard reads
	$(COMPOSE) run --rm spark /opt/payment-ledger/jobs/export_tables.py
	$(PY) -m payment_ledger.export

.PHONY: scenarios
scenarios:  ## List the six failures and what is supposed to catch each one
	$(PY) -m payment_ledger.chaos list

.PHONY: chaos
chaos: chaos-reset generate  ## Inject the six failures and check each was caught, and only it
	$(PY) -m payment_ledger.chaos_run --seed $(SEED)

.PHONY: chaos-one
chaos-one:  ## One arm end to end, into namespaces of its own (SCENARIO=dropped_event)
	$(PY) -m payment_ledger.chaos_run --only $(SCENARIO) --seed $(SEED)

.PHONY: chaos-reset
chaos-reset:  ## Drop what the suite wrote: its topics, checkpoints, namespaces, files and inputs
	$(PY) -m payment_ledger.chaos_run --reset

# The scheduler starts sibling containers through the docker socket, so what it hands the daemon
# are host paths and it has to be told where the tree is. On Windows this needs a Windows path:
# `LEDGER_HOST_ROOT=$$(pwd -W) make airflow` in Git Bash.
.PHONY: airflow
airflow:  ## Start the scheduler, with the chaos suite as a DAG, on localhost:8080
	LEDGER_HOST_ROOT="$${LEDGER_HOST_ROOT:-$(CURDIR)}" \
	  $(COMPOSE) --profile orchestration up -d airflow
	@echo "airflow: http://localhost:8080  user admin"
	@echo "password: docker exec pl-airflow cat /opt/airflow/standalone_admin_password.txt"

# The same two prerequisites `chaos` has, and for the same reason: an arm delivered on top of a
# previous suite publishes its events into a topic that already holds them. They stay here rather
# than becoming tasks because both talk to the docker CLI, which the scheduler does not have.
.PHONY: dag
dag: chaos-reset generate  ## Run the whole suite through Airflow instead of through the loop
	$(COMPOSE) exec airflow airflow dags trigger chaos_suite

.PHONY: sql
sql:  ## Open spark-sql against the lakehouse catalog
	$(COMPOSE) run --rm --entrypoint /opt/spark/bin/spark-sql spark

.PHONY: check
check: audit lint test  ## Everything CI runs

.PHONY: test
test:  ## Run the tests
	$(PY) -m pytest

.PHONY: lint
lint:  ## Lint and format-check
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
