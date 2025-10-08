"""
analyze_suggestions_clusters.py
-------------------------------------------------------------
Analyzes numeric constants from config_suggestions.py
and summarizes their distributions by constant type.

Example:
    python analyze_suggestions_clusters.py
-------------------------------------------------------------
"""

import re
import statistics
from pathlib import Path
import pandas as pd

path = Path("config_suggestions.py")

# Extract constants and numeric values from the file
pattern = re.compile(r"^(\w+)\s*=\s*([0-9.]+)$")
records = []

for line in path.read_text(encoding="utf-8").splitlines():
    match = pattern.match(line.strip())
    if match:
        name, val = match.groups()
        try:
            val = float(val)
        except ValueError:
            continue
        records.append({"name": name, "value": val})

if not records:
    print("⚠️ No numeric constants found in config_suggestions.py")
    raise SystemExit

df = pd.DataFrame(records)

# Group by name prefix (before _VAL)
df["base"] = df["name"].str.replace(r"_VAL$", "", regex=True)
summary = (
    df.groupby("base")["value"]
    .agg(["count", "min", "max", "mean", "median"])
    .sort_values(by="count", ascending=False)
)

print("\n📊 Constant Value Clusters:")
print(summary.round(3).to_string())

# Identify potential consolidation candidates
spread = (summary["max"] - summary["min"]).round(3)
high_variance = spread[spread > summary["mean"] * 0.25]

if not high_variance.empty:
    print("\n⚠️ Notable Variance — worth reviewing for consolidation:")
    for name, diff in high_variance.items():
        print(f"   {name:<25} range = {diff}")
else:
    print("\n✅ All constants are reasonably consistent.")
