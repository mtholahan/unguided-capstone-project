"""
QA_scan_magic_literals.py (numeric-only edition)
-------------------------------------------------------------
Scans all Step_XX Python scripts to identify numeric
"magic numbers" that should ideally live in config.py.

Outputs:
    • audit_reports/magic_numbers_audit.csv
    • audit_reports/magic_literals.json  (for config_reconciler.py)

Usage:
    python QA_scan_magic_literals.py
-------------------------------------------------------------
"""

import ast
import json
from pathlib import Path
import pandas as pd


# ============================================================
# 🔧 Configuration
# ============================================================
SCRIPTS_DIR = Path(r"C:\Projects\unguided-capstone-project\scripts")
OUTPUT_DIR = SCRIPTS_DIR / "audit_reports"
OUTPUT_DIR.mkdir(exist_ok=True)

CONFIG_FILE = SCRIPTS_DIR / "config.py"

EXCLUDE_FILES = {
    "base_step.py", "config.py", "utils.py", "main.py",
    "magic_number_audit.py", "QA_scan_magic_literals.py",
    "find_tqdm_hits.py", "step_99_ScratchPad.py", "__init__.py"
}

CSV_PATH = OUTPUT_DIR / "magic_numbers_audit.csv"
JSON_PATH = OUTPUT_DIR / "magic_literals.json"


# ============================================================
# 📘 Load existing config values
# ============================================================
def load_config_values():
    values = {}
    try:
        text = CONFIG_FILE.read_text(encoding="utf-8")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and isinstance(node.value, (ast.Constant, ast.Num)):
                        values[target.id] = node.value.value
    except Exception as e:
        print(f"⚠️ Could not parse config.py: {e}")
    return values


# ============================================================
# 🧠 AST Visitor for numeric constants
# ============================================================
class MagicNumberFinder(ast.NodeVisitor):
    def __init__(self, filename, config_values):
        self.filename = filename
        self.config_values = config_values
        self.results = []
        self.current_assign_target = None
        self.parent_func = None

    def visit_Assign(self, node):
        if isinstance(node.targets[0], ast.Name):
            self.current_assign_target = node.targets[0].id
        self.generic_visit(node)
        self.current_assign_target = None

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            self.parent_func = node.func.id
        self.generic_visit(node)
        self.parent_func = None

    def visit_Constant(self, node):
        val = node.value
        if not isinstance(val, (int, float)):
            return
        lineno = getattr(node, "lineno", -1)
        context = self._get_line_excerpt(lineno)

        # Filters
        if val in (0, 1, -1):  # trivial values
            return
        if self.parent_func in {"range", "enumerate", "len"}:
            return
        if self.current_assign_target and self.current_assign_target.isupper():
            return
        if val in self.config_values.values():
            return

        kind, suggestion = self.classify_number(val, context)
        self.results.append({
            "file": self.filename.name,
            "line": lineno,
            "value": val,
            "context": context,
            "kind": kind,
            "suggestion": suggestion
        })

    def _get_line_excerpt(self, lineno):
        try:
            lines = self.filename.read_text(encoding="utf-8").splitlines()
            if 0 < lineno <= len(lines):
                return lines[lineno - 1].strip()
        except Exception:
            return ""
        return ""

    def classify_number(self, val, context):
        ctx = context.lower()
        if any(k in ctx for k in ["threshold", "score", "cutoff", "<", ">"]):
            return ("threshold", "FUZZ_THRESHOLD")
        if any(k in ctx for k in ["sleep", "wait", "delay", "pause"]):
            return ("timing", "API_THROTTLE_SECONDS")
        if any(k in ctx for k in ["limit", "rows", "sample", "slice"]):
            return ("row limit", "ROW_LIMIT")
        if any(k in ctx for k in ["retry", "attempt"]):
            return ("retry", "MAX_RETRIES")
        if "timeout" in ctx:
            return ("timeout", "TIMEOUT_SECONDS")
        if "%" in ctx or "percent" in ctx:
            return ("percentage", "PERCENT_THRESHOLD")
        if any(k in ctx for k in ["multiplier", "scale", "factor"]):
            return ("scaling", "SCALE_FACTOR")
        return ("unknown", "")


# ============================================================
# 🚀 Main audit logic
# ============================================================
def main():
    print("🔍 Scanning for numeric magic literals...")

    config_values = load_config_values()
    step_files = [
        f for f in sorted(SCRIPTS_DIR.glob("step_*.py"))
        if f.name not in EXCLUDE_FILES
    ]

    print(f"🧭 Found {len(step_files)} Step scripts to audit:")
    for f in step_files:
        print(f"   • {f.name}")

    all_findings = []
    for f in step_files:
        try:
            tree = ast.parse(f.read_text(encoding="utf-8"))
        except SyntaxError as e:
            print(f"❌ Syntax error in {f.name}: {e}")
            continue

        finder = MagicNumberFinder(f, config_values)
        finder.visit(tree)
        all_findings.extend(finder.results)

    if not all_findings:
        print("✅ No numeric magic literals found.")
        return

    df = pd.DataFrame(all_findings)
    df.sort_values(by=["file", "line"], inplace=True)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")
    print(f"📊 Audit complete → {CSV_PATH}")

    # JSON output for config_reconciler.py
    JSON_PATH.write_text(json.dumps(all_findings, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"🧱 JSON audit data → {JSON_PATH}")

    print("\n📈 Summary:")
    kind_counts = df["kind"].value_counts().to_dict()
    for kind, count in kind_counts.items():
        print(f"   {kind:<15} {count:>3} found")

    print("\n💡 Tip: Focus first on 'threshold', 'limit', and 'timing' kinds.")


# ============================================================
# 🏁 Entry point
# ============================================================
if __name__ == "__main__":
    main()
