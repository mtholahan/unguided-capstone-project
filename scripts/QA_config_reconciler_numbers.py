"""
QA_config_reconciler_numbers.py (numeric-only)
-------------------------------------------------------------
Consumes magic_literals.json (from QA_scan_magic_literals.py)
and reconciles numeric constants against config.py.

Outputs:
    • <output_dir>/config_mapping.csv
    • <output_dir>/config_suggestions.py (alphabetically sorted)

Behavior:
    • Dedupes by (value, kind) pair
    • Scores similarity to existing numeric constants
    • "Match found" when similarity >= 80 (name-kind adds +10)
    • Otherwise proposes a readable constant name

Usage:
    python QA_config_reconciler_numbers.py \
        [--scripts-dir <scripts>] [--output-dir <out>] [--config-file <config.py>]
"""

import ast
import json
import argparse
from pathlib import Path
import pandas as pd


def default_paths():
    here = Path(__file__).resolve().parent
    scripts_dir = here
    out_dir = scripts_dir / "audit_reports"
    return scripts_dir, out_dir


def load_config_constants(config_file: Path):
    constants = {}
    try:
        text = config_file.read_text(encoding="utf-8")
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and isinstance(node.value, (ast.Constant, ast.Num)):
                        constants[target.id] = node.value.value
    except Exception as e:
        print(f"⚠️ Failed to parse {config_file.name}: {e}")
    return constants


def numeric_similarity(a, b) -> float:
    try:
        a, b = float(a), float(b)
        if a == b:
            return 100.0
        if a == 0:
            return 0.0
        diff = abs(a - b)
        return max(0.0, 100.0 - (diff / abs(a)) * 100.0)
    except Exception:
        return 0.0


def find_best_match(value, kind, config_constants: dict):
    best_name, best_score = "", 0.0
    for name, const_val in config_constants.items():
        score = numeric_similarity(value, const_val) if isinstance(const_val, (int, float)) else 0.0
        if kind and kind.lower() in name.lower():
            score += 10.0
        if score > best_score:
            best_name, best_score = name, score
    return best_name, round(best_score, 1)


def propose_new_constant(value, kind, context):
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


def main():
    parser = argparse.ArgumentParser(description="Reconcile numeric magic literals with config.py")
    scripts_dir_default, out_dir_default = default_paths()
    parser.add_argument("--scripts-dir", type=str, default=str(scripts_dir_default))
    parser.add_argument("--output-dir", type=str, default=str(out_dir_default))
    parser.add_argument("--config-file", type=str, default=str((scripts_dir_default / "config.py")))
    args = parser.parse_args()

    scripts_dir = Path(args.scripts_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    config_file = Path(args.config_file).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "magic_literals.json"
    if not json_path.exists():
        print(f"❌ Missing {json_path}. Run QA_scan_magic_literals.py first.")
        return

    config_constants = load_config_constants(config_file)

    data = json.loads(json_path.read_text(encoding="utf-8"))
    print(f"🔍 Loaded {len(data)} numeric constants from JSON")

    seen = set()
    reconciled = []

    for entry in data:
        val = entry.get("value")
        kind = entry.get("kind", "")
        context = entry.get("context", "")
        file = entry.get("file")
        line = entry.get("line")

        key = (val, kind)
        if key in seen:
            continue
        seen.add(key)

        best_name, score = find_best_match(val, kind, config_constants)
        if score >= 80:
            status, proposed = "Match found", best_name
        else:
            status, proposed = "New constant suggested", propose_new_constant(val, kind, context)

        reconciled.append({
            "file": file,
            "line": line,
            "value": val,
            "kind": kind,
            "context": context,
            "status": status,
            "proposed_name": proposed,
            "best_match": best_name,
            "similarity_score": score,
        })

    if not reconciled:
        print("✅ Nothing to reconcile.")
        return

    df = pd.DataFrame(reconciled)
    mapping_csv = output_dir / "config_mapping.csv"
    df.to_csv(mapping_csv, index=False, encoding="utf-8")
    print(f"📊 Mapping  → {mapping_csv}")

    # Alphabetically sorted suggestions (keep duplicates – variety is useful)
    suggestions = df[df["status"] == "New constant suggested"].copy()
    suggestions.sort_values(by="proposed_name", inplace=True, ignore_index=True)

    suggestions_py = output_dir / "config_suggestions.py"
    with suggestions_py.open("w", encoding="utf-8") as f:
        f.write("# Auto-generated config suggestions (numeric-only)\n")
        f.write("# Review before merging into config.py\n\n")
        for _, row in suggestions.iterrows():
            name = row["proposed_name"]
            val = row["value"]
            f.write(f"{name} = {val}\n")

    print(f"🧾 Suggestions → {suggestions_py}")

    print("\n📈 Summary:")
    print(df["status"].value_counts())

    kind_counts = df["kind"].value_counts().to_dict()
    print("\n🔢 Kind breakdown:")
    for k, c in kind_counts.items():
        print(f"   {k:<15} {c:>3} found")


if __name__ == "__main__":
    main()