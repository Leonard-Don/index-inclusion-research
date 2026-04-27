.PHONY: setup test lint serve coverage clean help rebuild verdicts doctor

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install the project in editable mode with dev dependencies
	python3 -m pip install -e ".[dev]"

lint: ## Run ruff linter
	python3 -m ruff check .

test: ## Run unit tests (excludes browser smoke)
	pytest -q

coverage: ## Run tests with coverage report
	pytest -q --cov=index_inclusion_research --cov-report=term-missing --cov-report=html:htmlcov

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

clean: ## Remove generated artifacts
	rm -rf htmlcov .coverage .pytest_cache .ruff_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
