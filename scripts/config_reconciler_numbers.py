"""
config_reconciler_numbers.py
-------------------------------------------------------------
Reads magic_literals.json produced by scan_magic_literals.py
(numeric-only version) and cross-references numeric constants
with existing config.py values.

Outputs:
    • audit_reports/config_mapping.csv
    • audit_reports/config_suggestions.py

Usage:
    python config_reconciler_numbers.py
-------------------------------------------------------------
"""

import ast
import json
import difflib
from pathlib import Path
import pandas as pd


# ============================================================
# 🔧 Configuration
# ============================================================
SCRIPTS_DIR = Path(r"C:\Projects\unguided-capstone-project\scripts")
AUDIT_DIR = SCRIPTS_DIR / "audit_reports"
AUDIT_DIR.mkdir(exist_ok=True)

CONFIG_FILE = SCRIPTS_DIR / "config.py"
MAGIC_JSON = AUDIT_DIR / "magic_literals.json"

MAPPING_CSV = AUDIT_DIR / "config_mapping.csv"
SUGGESTIONS_PY = AUDIT_DIR / "config_suggestions.py"


# ============================================================
# 📘 Load constants from config.py
# ============================================================
def load_config_constants():
    constants = {}
    try:
        text = CONFIG_FILE.read_text(encoding="utf-8")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and isinstance(node.value, (ast.Constant, ast.Num)):
                        constants[target.id] = node.value.value
    except Exception as e:
        print(f"⚠️ Could not parse config.py: {e}")
    return constants


# ============================================================
# 🔍 Load audit results
# ============================================================
def load_audit_results():
    if not MAGIC_JSON.exists():
        raise FileNotFoundError(f"Missing {MAGIC_JSON}. Run scan_magic_literals.py first.")
    try:
        return json.loads(MAGIC_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in {MAGIC_JSON}: {e}")


# ============================================================
# 🤖 Numeric comparison logic
# ============================================================
def numeric_similarity(a, b):
    """Return a 0–100 similarity score for numeric values."""
    try:
        a, b = float(a), float(b)
        diff = abs(a - b)
        if a == 0:
            return 0 if b != 0 else 100
        return max(0, 100 - (diff / abs(a)) * 100)
    except Exception:
        return 0


def find_best_match(value, kind, config_constants):
    """Return (match_name, match_score) for best fit."""
    best_match, best_score = "", 0.0
    for const_name, const_val in config_constants.items():
        if isinstance(const_val, (int, float)):
            score = numeric_similarity(value, const_val)
        else:
            score = 0
        if kind and kind.lower() in const_name.lower():
            score += 10
        if score > best_score:
            best_score, best_match = score, const_name
    return best_match, round(best_score, 1)


# ============================================================
# 🧠 Suggest new constant name
# ============================================================
def propose_new_constant(value, kind, context):
    """Infer a readable name for a new config constant."""
    ctx = (context or "").lower()
    if kind == "threshold" or "threshold" in ctx or "score" in ctx:
        base = "FUZZ_THRESHOLD"
    elif kind == "timing" or any(k in ctx for k in ["sleep", "wait", "pause"]):
        base = "API_THROTTLE_SECONDS"
    elif kind == "row limit" or "limit" in ctx:
        base = "ROW_LIMIT"
    elif kind == "retry" or "retry" in ctx:
        base = "MAX_RETRIES"
    elif "timeout" in ctx:
        base = "TIMEOUT_SECONDS"
    else:
        base = "MAGIC_CONST"
    return f"{base}_VAL"


# ============================================================
# 🧾 Reconcile & output results
# ============================================================
def reconcile_numbers():
    config_constants = load_config_constants()
    audit_results = load_audit_results()

    print(f"🔍 Loaded {len(audit_results)} numeric constants from JSON")

    seen = set()
    reconciled = []

    for entry in audit_results:
        val = entry.get("value")
        kind = entry.get("kind", "")
        context = entry.get("context", "")
        file = entry.get("file")
        line = entry.get("line")

        key = (val, kind)
        if key in seen:
            continue
        seen.add(key)

        best_match, score = find_best_match(val, kind, config_constants)

        if score >= 80:
            status, proposed_name = "Match found", best_match
        else:
            status, proposed_name = "New constant suggested", propose_new_constant(val, kind, context)

        reconciled.append({
            "file": file,
            "line": line,
            "value": val,
            "kind": kind,
            "context": context,
            "status": status,
            "proposed_name": proposed_name,
            "best_match": best_match,
            "similarity_score": score
        })

    if not reconciled:
        print("✅ No new numeric constants found.")
        return

    df = pd.DataFrame(reconciled)
    df.to_csv(MAPPING_CSV, index=False, encoding="utf-8")
    print(f"📊 Wrote config mapping → {MAPPING_CSV}")

    # Sort new constants alphabetically before writing
    new_constants = df[df["status"] == "New constant suggested"].copy()
    new_constants.sort_values(by="proposed_name", inplace=True, ignore_index=True)

    with open(SUGGESTIONS_PY, "w", encoding="utf-8") as f:
        f.write("# Auto-generated config suggestions (numeric-only)\n")
        f.write("# Review before merging into config.py\n\n")

        for _, row in new_constants.iterrows():
            name = row["proposed_name"]
            val = row["value"]

            # ensure nice string formatting
            if isinstance(val, str) and not val.startswith(("'", '"')):
                val = repr(val)

            f.write(f"{name} = {val}\n")

    print(f"🧾 Wrote {len(new_constants)} sorted constant suggestions → {SUGGESTIONS_PY}")

    print("\n📈 Summary:")
    print(df["status"].value_counts())

    print("\n🔢 Kind breakdown:")
    kind_counts = df["kind"].value_counts().to_dict()
    for k, c in kind_counts.items():
        print(f"   {k:<15} {c:>3} found")

    print("\n💡 Tip: prioritize constants with 'threshold', 'limit', or 'timing' kinds.")


# ============================================================
# 🏁 Entry point
# ============================================================
if __name__ == "__main__":
    reconcile_numbers()
