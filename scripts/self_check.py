import os, sys, pathlib, requests
from dotenv import load_dotenv

print("\n=== CONFIG_ENV INTEGRITY CHECK ===")
load_dotenv()

REQUIRED = ["TMDB_API_KEY", "TMDB_V4_TOKEN", "TMDB_BASE_URL", "OUTPUT_DIR"]
missing = [k for k in REQUIRED if not os.getenv(k)]
print("\nLoaded environment vars:")
for k in REQUIRED:
    print(f"  {k:<15}: {os.getenv(k)[:6] + '…' if os.getenv(k) else None}")

if missing:
    sys.exit(f"\n❌ Missing env vars: {missing}")

base = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3").rstrip("/")
k = os.getenv("TMDB_API_KEY")
v4 = os.getenv("TMDB_V4_TOKEN")

print("\nChecking endpoint reachability…")
try:
    r3 = requests.get(f"{base}/configuration", params={"api_key": k}, timeout=10)
    print("v3 status:", r3.status_code)
    r4 = requests.get(f"{base}/movie/550", headers={"Authorization": f"Bearer {v4}"}, timeout=10)
    print("v4 status:", r4.status_code)
except Exception as e:
    sys.exit(f"❌ Connection error: {e}")

out = pathlib.Path(os.getenv("OUTPUT_DIR", "data/output")).resolve()
out.mkdir(parents=True, exist_ok=True)
testfile = out / "_write_test.tmp"
try:
    testfile.write_text("ok")
    print(f"✅ Output path writable: {out}")
    testfile.unlink()
except Exception as e:
    sys.exit(f"❌ Write test failed: {e}")

print("\n✅ All core config_env checks passed.\n")
