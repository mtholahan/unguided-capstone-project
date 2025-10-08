# 📘 QA Suite Guide — *How & Why*

### Why this suite exists

- **Clarity & Maintainability**: Magic numbers hide intent. Promoting them into `config.py` makes behavior explicit and consistent.
- **Observability**: These tools quantify *where* thresholds/limits/timeouts live, and how consistent they are.
- **Governance**: Over time, config files accrete unused constants. This suite detects both *missing* and *unused* configuration.

### Typical workflow

1. **Scan for numeric literals**  
   `python QA_scan_magic_literals.py`  
   → Produces `QA/audit_reports/magic_numbers_audit.csv` + `magic_literals.json`

2. **Reconcile with config.py**  
   `python QA_config_reconciler_numbers.py`  
   → Produces `QA/audit_reports/config_mapping.csv` + `config_suggestions.py` (sorted)

3. **Assess spread/consistency** *(optional)*  
   `python QA_analyze_suggestions_clusters.py`  
   → Console table showing clusters/ranges to guide consolidation

4. **Audit actual config usage**  
   `python QA_config_usage_audit.py`  
   → CSV + color-coded console grouping of unused constants by category

### Where files live

- All scripts default to the folder of the script (`Path(__file__).parent`).
- Outputs default to `./audit_reports` next to the scripts.  
- Override paths with `--scripts-dir`, `--output-dir`, `--config-file` as needed.

### Safety/Edge Cases

- All scripts use UTF‑8 reads/writes.
- Scanner gracefully skips files with syntax errors and continues.
- Reconciler preserves duplicate suggestions (helps you see variety).

### CI / Pre‑commit ideas

- Run the scanner + reconciler in CI and fail the build if new *unknown* kinds appear (e.g., a sudden new class of thresholds).
- Run the usage audit and warn if unused constants exceed N.

### Azure readiness

- Paths are relative; safe to run in containers or Azure CI.
- To persist outputs, point `--output-dir` at a mounted volume or blob-backed path.

---

**That’s it!** You now have a cohesive, ergonomic QA suite for keeping configuration *tight*, *explainable*, and *actionable* across your pipeline.







