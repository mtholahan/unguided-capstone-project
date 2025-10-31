import os
from dotenv import dotenv_values
from pyspark.sql import SparkSession

def load_and_validate_env():
    env = dotenv_values("/home/mark/Projects/unguided-capstone-project/.env")
    return env

def build_spark_session():
    env = load_and_validate_env()
    account = env.get("AZURE_STORAGE_ACCOUNT_NAME")
    PYSPARK_PYTHON = "/home/mark/pyspark_venv311/bin/python"

    spark_builder = (
        SparkSession.builder
        .appName("UnguidedCapstone_Pipeline")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.pyspark.python", PYSPARK_PYTHON)
        .config("spark.pyspark.driver.python", PYSPARK_PYTHON)
        .config("spark.executorEnv.PYSPARK_PYTHON", PYSPARK_PYTHON)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", PYSPARK_PYTHON)
        .config("fs.azure.account.auth.type", "OAuth")
        .config("fs.azure.account.oauth.provider.type",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        .config("fs.azure.account.oauth2.client.id", env["AZURE_APP_ID"])
        .config("fs.azure.account.oauth2.client.secret", env["AZURE_APP_SECRET"])
        .config("fs.azure.account.oauth2.client.endpoint",
                f"https://login.microsoftonline.com/{env['AZURE_TENANT_ID']}/oauth2/token")
    )


    # --- Primary: OAuth (Service Principal) ---
    try:
        spark_builder = (
            spark_builder
            .config(f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net", "OAuth")
            .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{account}.dfs.core.windows.net",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{account}.dfs.core.windows.net",
                    env["AZURE_APP_ID"])
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{account}.dfs.core.windows.net",
                    env["AZURE_APP_SECRET"])
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{account}.dfs.core.windows.net",
                    f"https://login.microsoftonline.com/{env['AZURE_TENANT_ID']}/oauth2/token")
        )
    except KeyError:
        # --- Fallback: Shared Key (if OAuth unavailable) ---
        spark_builder = (
            spark_builder
            .config(f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SharedKey")
            .config(f"spark.hadoop.fs.azure.account.key.{account}.dfs.core.windows.net",
                    env["AZURE_STORAGE_ACCOUNT_KEY"])
        )

    spark = spark_builder.getOrCreate()
    return spark
