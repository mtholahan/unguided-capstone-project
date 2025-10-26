# ===================================================
#   Springboard Unguided Capstone: Test Automation
# ===================================================

PYTHON := python3

.PHONY: help test coverage clean

help:
	@echo "Usage:"
	@echo "  make test        -> run all tests"
	@echo "  make coverage    -> run tests with coverage report"
	@echo "  make clean       -> remove __pycache__ and coverage artifacts"

test:
	@echo "🧪 Running unit + integration tests..."
	pytest

coverage:
	@echo "📊 Running coverage report..."
	pytest --cov=scripts --cov-report=term-missing

clean:
	@echo "🧹 Cleaning build/test artifacts..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .coverage htmlcov
