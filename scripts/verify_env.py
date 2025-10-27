import importlib
import importlib.util
import os
import sys

REQUIRED_MODULES = [
    "pandas",
    "pyspark",
    "rapidfuzz",
    "matplotlib",
]

REQUIRED_ENV_VARS = [
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_KEY",
    "AZURE_STORAGE_CONTAINER",
]

missing_modules = [m for m in REQUIRED_MODULES if importlib.util.find_spec(m) is None]
missing_env = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]

if missing_modules or missing_env:
    if missing_modules:
        print(f"❌ Missing Python modules: {', '.join(missing_modules)}")
    if missing_env:
        print(f"❌ Missing environment variables: {', '.join(missing_env)}")
    sys.exit(1)

print("✅ Environment verified: all required modules and variables are present.")
sys.exit(0)
