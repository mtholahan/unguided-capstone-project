# scripts/utils/env.py
from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv

def load_env():
    """
    Loads environment variables from the nearest .env file,
    auto-detecting the project root when running locally or on Azure.
    """
    # 1️⃣ Find the nearest .env file
    dotenv_path = find_dotenv(usecwd=True)
    load_dotenv(dotenv_path)

    # 2️⃣ Build env dict after dotenv load
    env = dict(os.environ)

    # 3️⃣ Resolve ROOT automatically
    #    If .env not found or ROOT missing, default to 3 levels up from this file
    repo_root = Path(dotenv_path).parent if dotenv_path else Path(__file__).resolve().parents[2]
    env.setdefault("ROOT", str(repo_root))

    # 4️⃣ Normalize key names for backward compatibility
    if "root" not in env:
        env["root"] = env["ROOT"]

    return env
