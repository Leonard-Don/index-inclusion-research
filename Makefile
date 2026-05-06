.PHONY: setup sync lock test lint typecheck quality coverage-gate ci serve coverage clean help rebuild verdicts doctor doctor-strict paper paper-audit pre-commit-install pre-commit-run

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install the project in editable mode with dev dependencies
	python3 -m pip install -e ".[dev]"

sync: ## Install pinned dependencies from uv.lock (reproducible)
	uv sync --extra dev

lock: ## Refresh uv.lock from pyproject.toml
	uv lock

lint: ## Run ruff linter
	python3 -m ruff check .

typecheck: ## Run mypy on the package source
	python3 -m mypy src/index_inclusion_research

test: ## Run unit tests (excludes browser smoke)
	pytest -q

quality: lint typecheck test doctor-strict ## Run the local pre-commit quality gate

coverage: ## Run tests with coverage report
	pytest -q --cov=index_inclusion_research --cov-report=term-missing --cov-report=html:htmlcov

coverage-gate: ## Run tests with the CI coverage threshold
	pytest -q --cov=index_inclusion_research --cov-report=term-missing --cov-report=xml:coverage.xml --cov-fail-under=70

ci: lint typecheck coverage-gate doctor-strict ## Run the CI-equivalent non-browser quality gate

smoke: ## Run dashboard browser smoke tests (requires Playwright + Chromium)
	python3 -m playwright install chromium
	RUN_BROWSER_SMOKE=1 pytest -q -m browser_smoke tests/test_dashboard_browser_smoke.py

serve: ## Start the dashboard on localhost:5001
	index-inclusion-dashboard

rebuild: ## Run the full pipeline (events → CMA → figures → research report)
	index-inclusion-rebuild-all

verdicts: ## Print the current 7-hypothesis verdict picture
	index-inclusion-verdict-summary

doctor: ## Run project health checks (paper IDs / CSVs / chart registry / scripts)
	index-inclusion-doctor

doctor-strict: ## Run project health checks with --fail-on-warn (CI-strict)
	index-inclusion-doctor --fail-on-warn

paper: ## Aggregate paper-grade artefacts into ./paper/ (tables / figures / narrative / RDD)
	python3 -m index_inclusion_research.paper_bundle --force
	python3 -m index_inclusion_research.paper_audit --fail-on-warn

paper-audit: ## Audit paper claims against result artifacts and bundle copies
	python3 -m index_inclusion_research.paper_audit --fail-on-warn

pre-commit-install: ## Install pre-commit git hooks (one-time, per clone)
	pre-commit install

pre-commit-run: ## Run all pre-commit hooks against the working tree
	pre-commit run --all-files

clean: ## Remove generated artifacts
	rm -rf htmlcov .coverage .pytest_cache .ruff_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
