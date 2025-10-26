import importlib, sys
modules = ["pandas", "pyspark", "rapidfuzz"]
missing = [m for m in modules if importlib.util.find_spec(m) is None]
if missing:
    sys.exit(f"Missing critical modules: {', '.join(missing)}")
print("✅ Environment verified.")
