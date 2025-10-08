"""
QA_analyze_suggestions_clusters.py
-------------------------------------------------------------
Analyzes numeric constants proposed in config_suggestions.py
and summarizes distributions by base name (before _VAL).

Outputs: console table

Usage:
    python QA_analyze_suggestions_clusters.py \
        [--suggestions-file <output_dir>/config_suggestions.py]
"""

import re
import argparse
from pathlib import Path
import statistics
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Analyze distribution of proposed config constants")
    parser.add_argument("--suggestions-file", type=str, default="audit_reports/config_suggestions.py")
    args = parser.parse_args()

    path = Path(args.suggestions_file).resolve()
    if not path.exists():
        print(f"❌ Missing {path}. Run QA_config_reconciler_numbers.py first.")
        return

    pattern = re.compile(r"^(\w+)\s*=\s*([0-9.]+)$")
    records = []

    for line in path.read_text(encoding="utf-8").splitlines():
        m = pattern.match(line.strip())
        if m:
            name, val = m.groups()
            try:
                val = float(val)
            except ValueError:
                continue
            records.append({"name": name, "value": val})

    if not records:
        print("⚠️ No numeric constants found in config_suggestions.py")
        return

    df = pd.DataFrame(records)
    df["base"] = df["name"].str.replace(r"_VAL$", "", regex=True)
    summary = (
        df.groupby("base")["value"].agg(["count", "min", "max", "mean", "median"]).sort_values(by="count", ascending=False)
    )

    print("\n📊 Constant Value Clusters:")
    print(summary.round(3).to_string())

    spread = (summary["max"] - summary["min"]).round(3)
    high_variance = spread[spread > summary["mean"] * 0.25]
    if not high_variance.empty:
        print("\n⚠️ Notable Variance — worth reviewing for consolidation:")
        for name, diff in high_variance.items():
            print(f"   {name:<25} range = {diff}")
    else:
        print("\n✅ All constants are reasonably consistent.")


if __name__ == "__main__":
    main()