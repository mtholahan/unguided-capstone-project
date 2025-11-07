# ================================================================
# step_04_validate_schema_alignment.py — v4.1
# ================================================================

import scripts.bootstrap
from scripts import config
from scripts.base_step import BaseStep
from pyspark.sql import SparkSession
import pandas as pd, json, time, fsspec
from scripts.utils_schema import infer_schema, build_integrity_summary


class Step04ValidateSchemaAlignment(BaseStep):
    def __init__(self):
        super().__init__("step_04_validate_schema_alignment")
        self.metrics_dir = config.METRICS_DIR
        self.use_spark = getattr(config, "USE_SPARK_VALIDATION", False)
        self.spark = SparkSession.builder.getOrCreate() if self.use_spark else None
        self.fs = fsspec.filesystem("abfss", account_name=config.STORAGE_ACCOUNT, anon=False)

    def _read_layer(self, layer: str, dataset: str):
        path = config.layer_path(layer, dataset)
        if self.use_spark:
            return self.spark.read.parquet(path)
        return pd.read_parquet(path, storage_options={"account_name": config.STORAGE_ACCOUNT, "anon": False})

    def run(self):
        t0 = time.time()
        tmdb_df = self._read_layer("bronze", "tmdb")
        discogs_df = self._read_layer("bronze", "discogs")
        candidates_df = self._read_layer("silver", "candidates")

        if self.use_spark:
            tmdb_pd = tmdb_df.limit(5000).toPandas()
            discogs_pd = discogs_df.limit(5000).toPandas()
            candidates_pd = candidates_df.limit(5000).toPandas()
        else:
            tmdb_pd, discogs_pd, candidates_pd = tmdb_df, discogs_df, candidates_df

        tmdb_schema = infer_schema(tmdb_pd)
        discogs_schema = infer_schema(discogs_pd)
        integrity_df = build_integrity_summary(tmdb_pd, discogs_pd, candidates_pd, self.logger)

        schema_map = {
            "tmdb": tmdb_schema.to_dict(orient="records"),
            "discogs": discogs_schema.to_dict(orient="records"),
            "integrity": integrity_df.to_dict(orient="records"),
            "metadata": {
                "validated_on": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "env": config.ENV,
                "run_id": config.RUN_ID,
            },
        }

        validation_dir = config.layer_path("silver", "validation/schema_alignment")
        self.fs.makedirs(validation_dir, exist_ok=True)

        with self.fs.open(f"{validation_dir}/schema_tmdb.csv", "wb") as f:
            tmdb_schema.to_csv(f, index=False)
        with self.fs.open(f"{validation_dir}/schema_discogs.csv", "wb") as f:
            discogs_schema.to_csv(f, index=False)
        with self.fs.open(f"{validation_dir}/integrity_summary.csv", "wb") as f:
            integrity_df.to_csv(f, index=False)
        with self.fs.open(f"{validation_dir}/schema_map.json", "w") as f:
            json.dump(schema_map, f, indent=2)

        metrics = {
            "tmdb_rows": len(tmdb_pd),
            "discogs_rows": len(discogs_pd),
            "candidate_rows": len(candidates_pd),
            "tmdb_columns": tmdb_pd.shape[1],
            "discogs_columns": discogs_pd.shape[1],
            "duration_sec": round(time.time() - t0, 1),
            "validation_dir": validation_dir,
        }
        self.write_metrics(metrics, "step04_schema_validation_metrics", self.metrics_dir)
        return integrity_df
