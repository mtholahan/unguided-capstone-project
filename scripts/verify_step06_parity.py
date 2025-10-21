# scripts/verify_step06_parity.py
"""
Verifies parity between Step05 (local) and Step06 (Spark) match results.
Compares score statistics, overlap of high-confidence matches (>= 90),
and logs results to JSON and histogram.
"""

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import json, time

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
BASE = Path("data/intermediate")
METRICS = Path("data/metrics")
F05 = BASE / "tmdb_discogs_matches_v2.csv"
F06 = BASE / "tmdb_discogs_matches_spark.csv"
OUT_JSON = METRICS / "step06_parity.json"
OUT_PNG = METRICS / "step06_parity_check.png"

t0 = time.time()

# ------------------------------------------------------------
# Load data
# ------------------------------------------------------------
df05 = pd.read_csv(F05)
df06 = pd.read_csv(F06)

m05, s05 = df05["hybrid_score"].mean(), df05["hybrid_score"].std()
m06, s06 = df06["hybrid_score"].mean(), df06["hybrid_score"].std()
delta = round(abs(m06 - m05), 2)

print(f"Step05 → mean={m05:.2f}, std={s05:.2f}, n={len(df05)}")
print(f"Step06 → mean={m06:.2f}, std={s06:.2f}, n={len(df06)}")

status = "ok" if delta <= 2 else "warning"
msg = "✅ Step06 parity within ±2 points" if status == "ok" else f"⚠️ Mean differs by {delta:.2f} points"
print(msg)

# ------------------------------------------------------------
# High-confidence overlap analysis (>=90)
# ------------------------------------------------------------
high05 = df05.query("hybrid_score >= 90")[["tmdb_id", "discogs_id"]].dropna()
high06 = df06.query("hybrid_score >= 90")[["tmdb_id", "discogs_id"]].dropna()

set05 = set(zip(high05.tmdb_id, high05.discogs_id))
set06 = set(zip(high06.tmdb_id, high06.discogs_id))

overlap = set05 & set06
jaccard = round(len(overlap) / len(set05 | set06), 3) if (set05 or set06) else 0.0

print(f"High-confidence matches → Step05={len(set05)}, Step06={len(set06)}, Overlap={len(overlap)}")
print(f"Jaccard similarity = {jaccard:.3f}")

# ------------------------------------------------------------
# Histogram overlay
# ------------------------------------------------------------
plt.figure(figsize=(8,5))
plt.hist(df05["hybrid_score"], bins=20, alpha=0.5, label="Step05 (local)")
plt.hist(df06["hybrid_score"], bins=20, alpha=0.5, label="Step06 (Spark)")
plt.legend()
plt.title("Step05 vs Step06 Score Distribution")
plt.xlabel("Hybrid Score"); plt.ylabel("Frequency")
plt.tight_layout()
METRICS.mkdir(parents=True, exist_ok=True)
plt.savefig(OUT_PNG)
plt.close()

# ------------------------------------------------------------
# JSON summary
# ------------------------------------------------------------
parity_metrics = {
    "step05": {"mean": round(m05,2), "stddev": round(s05,2), "count": len(df05)},
    "step06": {"mean": round(m06,2), "stddev": round(s06,2), "count": len(df06)},
    "mean_delta": delta,
    "status": status,
    "message": msg,
    "high_confidence_overlap": {
        "step05_count": len(set05),
        "step06_count": len(set06),
        "overlap_count": len(overlap),
        "jaccard_similarity": jaccard
    },
    "runtime_sec": round(time.time() - t0, 2),
    "source_files": [str(F05), str(F06)]
}

with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(parity_metrics, f, indent=2)

print(f"📈 Saved parity metrics → {OUT_JSON}")
print(f"📊 Saved comparison histogram → {OUT_PNG}")
