import os
from pyspark.sql import SparkSession

# Resolve path relative to project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(project_root, "data", "mock", "tmdb")

spark = SparkSession.builder.appName("verify_tmdb_output").getOrCreate()

df = spark.read.parquet(data_path)
print("✅ Row count:", df.count())
print("🧱 Schema:")
df.printSchema()
print("🔍 Sample rows:")
df.show(5, truncate=False)

spark.stop()
