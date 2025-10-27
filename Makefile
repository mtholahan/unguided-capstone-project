# ============================================================
# 🎬 Unguided Capstone Project Makefile
# Step 8: Deploy Your Code for Testing
# ============================================================

# Python virtual environment
VENV ?= pyspark_venv311
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

# Default directories
SRC_DIR = scripts
TEST_DIR = $(SRC_DIR)/tests
DATA_DIR = data
REPORTS_DIR = reports
LOGS_DIR = logs

# ============================================================
# 🧱 Environment Setup
# ============================================================

.PHONY: setup
setup:
	@echo "🔧 Setting up environment..."
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@mkdir -p $(REPORTS_DIR) $(LOGS_DIR) $(DATA_DIR)/intermediate $(DATA_DIR)/metrics
	@echo "✅ Environment ready."

# ============================================================
# 🧹 Code Quality & Linting
# ============================================================

.PHONY: lint format check
lint:
	@echo "🔍 Running flake8 lint checks..."
	$(PYTHON) -m flake8 $(SRC_DIR) --count --max-line-length=100 --statistics

format:
	@echo "🧽 Auto-formatting code with black..."
	$(PYTHON) -m black $(SRC_DIR)

check:
	@echo "🧠 Static type checking..."
	$(PYTHON) -m mypy $(SRC_DIR)

# ============================================================
# 🧪 Testing & Validation
# ============================================================

.PHONY: test test-step8 coverage

# Run fast unit tests only (no Spark jobs)
test:
	@echo "🧩 Running unit tests..."
	pytest -m "not integration" -q

# Run full Step 8 integration pipeline tests
test-step8:
	@echo "🚀 Running Step 8 integration test suite..."
	pytest -m integration --maxfail=1 --disable-warnings --tb=short
	@echo "✅ Step 8 integration tests completed."

# Generate coverage report (terminal + HTML)
coverage:
	@echo "📊 Running coverage analysis..."
	pytest --cov=$(SRC_DIR) --cov-report=term-missing --cov-report=html:$(REPORTS_DIR)/htmlcov -s
	@echo "✅ Coverage report → $(REPORTS_DIR)/htmlcov/index.html"

# ============================================================
# 🧾 Reports & Logs
# ============================================================

.PHONY: clean clean-data logs

clean:
	@echo "🧹 Cleaning temporary files..."
	rm -rf $(REPORTS_DIR) $(LOGS_DIR) .pytest_cache .coverage
	@echo "✅ Cleaned build and test artifacts."

clean-data:
	@echo "🧹 Cleaning data/intermediate and metrics directories..."
	rm -rf $(DATA_DIR)/intermediate/* $(DATA_DIR)/metrics/*
	@echo "✅ Cleaned pipeline data outputs."

logs:
	@echo "📂 Tailing latest pipeline test log..."
	@tail -n 30 $(TEST_DIR)/../pipeline_test.log || echo "No log file found."

# ============================================================
# ☁️ Azure / Deployment Utilities
# ============================================================

.PHONY: azure-login sync-data run-azure

azure-login:
	@echo "🔐 Logging in to Azure CLI..."
	az login --use-device-code

sync-data:
	@echo "☁️ Syncing local data to Azure Storage..."
	az storage blob upload-batch -d "\$$PIPELINE_CONTAINER" -s $(DATA_DIR)

run-azure:
	@echo "🚀 Submitting pipeline to Azure Databricks or compute..."
	$(PYTHON) $(SRC_DIR)/main.py --output-dir $(DATA_DIR)/intermediate

# ============================================================
# 📦 Packaging & Submission
# ============================================================

.PHONY: package

package:
	@echo "📦 Packaging Step 8 deliverables..."
	mkdir -p $(REPORTS_DIR)/submission
	cp -r $(REPORTS_DIR)/htmlcov $(REPORTS_DIR)/submission/coverage_html
	cp -r $(DATA_DIR)/metrics $(REPORTS_DIR)/submission/metrics
	cp $(TEST_DIR)/../pipeline_test_report.json $(REPORTS_DIR)/submission/ || true
	@echo "✅ Deliverable bundle ready in $(REPORTS_DIR)/submission/"
