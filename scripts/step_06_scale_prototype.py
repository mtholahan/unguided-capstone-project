"""
Step 06 – Scale Your Prototype (Refactored Baseline)
Stable for PySpark 3.5 + Pandas 2.x on Python 3.11 (local or Azure)
"""

# ================================================================
#  Imports & Pandas Index Serialization Patch
# ================================================================
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.pandas.functions import pandas_udf
from pathlib import Path
from rapidfuzz import fuzz
import matplotlib.pyplot as plt
import numpy as np, pandas as pd, json, time, copyreg, os

# --- Patch for pandas.core.indexes.base._new_Index ---
from pandas.core.indexes.base import _new_Index  # type: ignore
def _construct_new_index():
    return _new_Index.__new__(_new_Index)
copyreg.pickle(_new_Index, lambda obj: (_construct_new_index, ()))

# Propagate patch to executors
os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", "python3")
os.environ["SPARK_ALLOW_INCOMPATIBLE_PANDAS_VERSION"] = "true"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# ================================================================
#  Paths & Directories
# ================================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data"
INPUT        = DATA_DIR / "intermediate" / "tmdb_discogs_candidates_extended.csv"
OUTPUT_DIR   = DATA_DIR / "intermediate"
OUTPUT       = OUTPUT_DIR / "tmdb_discogs_matches_spark.csv"
METRICS_DIR  = DATA_DIR / "metrics"
METRICS_JSON = METRICS_DIR / "step06_spark_metrics.json"
HISTOGRAM    = METRICS_DIR / "step06_spark_score_distribution.png"
METRICS_DIR.mkdir(parents=True, exist_ok=True)

# ================================================================
#  Spark Session
# ================================================================
spark = (
    SparkSession.builder
    .appName("Step06_ScalePrototype")
    .config("spark.master", "local[2]")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.addPyFile(__file__)

t0 = time.time()
print(f"🚀 Step 06 start | Input → {INPUT}")

# ================================================================
#  Load & Clean Data
# ================================================================
if not INPUT.exists():
    raise FileNotFoundError(f"❌ Missing input file: {INPUT}")

df_raw = spark.read.option("header", True).csv(str(INPUT))
df = (
    df_raw
    .replace("None", None)
    .replace("null", None)
    .replace("", None)
    .withColumn("tmdb_year",
                F.when(F.col("tmdb_year").rlike("^[0-9]+$"),
                       F.col("tmdb_year").cast(T.IntegerType())))
    .withColumn("discogs_year",
                F.when(F.col("discogs_year").rlike("^[0-9]+$"),
                       F.col("discogs_year").cast(T.IntegerType())))
)
count_in = df.count()
print(f"📥 Loaded {count_in:,} records")

# ================================================================
#  Hybrid Fuzzy Matcher (UDF)
# ================================================================
@pandas_udf("double")
def hybrid_score_udf(t1: pd.Series, t2: pd.Series, y1: pd.Series, y2: pd.Series) -> pd.Series:
    from rapidfuzz import fuzz
    scores = []
    for a, b, ya, yb in zip(t1.fillna(""), t2.fillna(""), y1.fillna(0), y2.fillna(0)):
        try:
            if not a.strip() or not b.strip():
                scores.append(0.0);  continue
            if ya and yb and abs(int(ya) - int(yb)) > 1:
                scores.append(0.0);  continue
            token  = fuzz.token_sort_ratio(a, b)
            part   = fuzz.partial_ratio(a, b)
            scores.append(round(0.7 * token + 0.3 * part, 2))
        except Exception:
            scores.append(0.0)
    return pd.Series(scores)

# ================================================================
#  Apply Matcher & Filter
# ================================================================
df_matched = (
    df.withColumn("hybrid_score",
        hybrid_score_udf("tmdb_title_norm", "discogs_title_norm",
                         "tmdb_year", "discogs_year"))
      .filter(F.col("hybrid_score") > 0)
)
count_out = df_matched.count()
print(f"✅ Matches computed → {count_out:,}")

# ================================================================
#  Write Output
# ================================================================
tmp = OUTPUT_DIR / "_tmp_step06"
(
    df_matched.coalesce(1)
    .write.mode("overwrite").option("header", True)
    .csv(str(tmp))
)
# Move single CSV out of temporary dir
for f in tmp.glob("part-*.csv"):
    f.replace(OUTPUT)
for extra in tmp.glob("*"):  extra.unlink()
tmp.rmdir()

# ================================================================
#  Metrics & Visualization
# ================================================================
pdf = df_matched.sample(fraction=0.2, seed=42).toPandas()
avg, med = pdf["hybrid_score"].mean(), pdf["hybrid_score"].median()
plt.figure(figsize=(8,5))
plt.hist(pdf["hybrid_score"], bins=20, edgecolor="black")
plt.title("Step 06 – Hybrid Score Distribution")
plt.xlabel("Hybrid Score"); plt.ylabel("Frequency")
plt.tight_layout(); plt.savefig(HISTOGRAM); plt.close()

metrics = {
    "total_candidates": int(count_in),
    "total_matches": int(count_out),
    "avg_hybrid_score": round(float(avg), 2),
    "median_hybrid_score": round(float(med), 2),
    "runtime_sec": round(time.time() - t0, 2)
}
with open(METRICS_JSON, "w", encoding="utf-8") as f:
    json.dump(metrics, f, indent=2)

print(f"📈 Metrics → {METRICS_JSON}")
print(f"📊 Histogram → {HISTOGRAM}")
spark.stop()
print("🏁 Step 06 complete")
