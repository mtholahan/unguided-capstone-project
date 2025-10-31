"""
main.py — TMDB / Discogs Unified Pipeline (v8)
------------------------------------------------------------
Delegates environment + Spark configuration to config_env.py.
Author: Mark Holahan
Version: v8 (stable baseline, Oct 2025)
"""

from datetime import datetime
from config_env import build_spark_session, load_and_validate_env

print("🚀 Starting Spark orchestrator (v8 baseline)...")

env = load_and_validate_env()

print("🔍 AZURE_STORAGE_ACCOUNT_NAME:", env.get("AZURE_STORAGE_ACCOUNT_NAME"))

# ================================================================
# 1️⃣  Spark + Environment Initialization
# ================================================================
spark = build_spark_session()
print("✅ Spark initialized and connected to ADLS.")

# ================================================================
# 2️⃣  ADLS Connectivity Test
# ================================================================
ACCOUNT = spark.conf.get("spark.hadoop.fs.azure.account.auth.type.ungcaptor01.dfs.core.windows.net", "unknown")
CONTAINER = "raw"
test_path = f"abfss://{CONTAINER}@ungcaptor01.dfs.core.windows.net/test_write_check"

print(f"🧪 Testing ADLS write to: {test_path}")
try:
    spark.createDataFrame(
        [(datetime.now().isoformat(), "ok")], ["timestamp", "status"]
    ).write.mode("overwrite").parquet(test_path)
    print(f"✅ Verified ADLS write to: {test_path}")
except Exception as e:
    print(f"❌ ADLS write test failed: {e}")
    spark.stop()
    raise SystemExit(1)

# ================================================================
# 3️⃣  Pipeline Step Execution
# ================================================================
try:
    from scripts_spark.extract_spark_tmdb import ExtractSparkTMDB
    from scripts_spark.extract_spark_discogs import ExtractSparkDiscogs
except ModuleNotFoundError as e:
    print(f"⚠️  Could not import step modules: {e}")
    spark.stop()
    raise SystemExit(1)

steps = [ExtractSparkTMDB(spark), ExtractSparkDiscogs(spark)]

for step in steps:
    print(f"🚩 Running {step.__class__.__name__}")
    try:
        step.run()
        print(f"✅ Finished {step.__class__.__name__}")
    except Exception as e:
        print(f"❌ Step {step.__class__.__name__} failed: {e}")
        break

# ================================================================
# 4️⃣  Clean Shutdown
# ================================================================
spark.stop()
print("🏁 Pipeline completed cleanly.")
