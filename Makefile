# Clara — common tasks.
#
#   make install   set up a dev environment
#   make demo      end-to-end pipeline on sample data, no credentials needed
#   make serve     API + web console at http://localhost:8080
#   make test      run the suite
#   make stack     full containerised stack (MinIO + Iceberg + Trino + Clara)

PYTHON ?= python3.12
VENV   := .venv
BIN    := $(VENV)/bin
COMPOSE := docker compose -f deploy/docker-compose.yml

# Local single-node defaults: no cloud account, no containers.
LOCAL_ENV := CLARA_ENV=local CLARA_PROVIDER=local CLARA_CATALOG_KIND=sqlite CLARA_AUTH_DISABLED=true

.DEFAULT_GOAL := help
.PHONY: help install demo serve test lint format typecheck clean stack stack-down stack-logs

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

$(BIN)/clara: pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --quiet --upgrade pip setuptools wheel
	$(BIN)/pip install --quiet -e ".[duckdb,trino,iceberg,s3,postgres,dev]"
	@touch $(BIN)/clara

install: $(BIN)/clara  ## Create the dev environment
	@echo "ready — try: make demo"

demo: install  ## Run the sample pipeline end to end
	@$(LOCAL_ENV) $(BIN)/clara init --force
	@$(LOCAL_ENV) $(BIN)/clara -q run
	@$(LOCAL_ENV) $(BIN)/clara -q table list
	@$(LOCAL_ENV) $(BIN)/clara -q query "SELECT * FROM analytics.daily_revenue ORDER BY revenue DESC LIMIT 5"

serve: install  ## Start the API and web console
	@$(LOCAL_ENV) $(BIN)/clara serve

test: install  ## Run the test suite
	@$(BIN)/python -m pytest -q

lint: install  ## Lint
	@$(BIN)/ruff check src tests

format: install  ## Auto-fix lint and format
	@$(BIN)/ruff check --fix src tests
	@$(BIN)/ruff format src tests

typecheck: install  ## Type-check
	@$(BIN)/mypy src

stack:  ## Start the full containerised stack
	$(COMPOSE) up -d --build
	@echo "console:  http://localhost:8000"
	@echo "trino:    http://localhost:8080"
	@echo "minio:    http://localhost:9001  (clara / clara-dev-secret)"

stack-down:  ## Stop the stack and remove volumes
	$(COMPOSE) down -v

stack-logs:  ## Tail stack logs
	$(COMPOSE) logs -f --tail=100

clean:  ## Remove local state and build artefacts
	rm -rf .clara .pytest_cache .ruff_cache .mypy_cache dist build *.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
