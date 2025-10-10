"""
Quick Discogs API Auth Test
Verifies that DISCOGS_CONSUMER_KEY and DISCOGS_CONSUMER_SECRET are valid
and that you can access /database/search successfully.
"""

import os
import sys
import requests
from dotenv import load_dotenv

# Load environment from .env at project root
load_dotenv()

DISCOGS_CONSUMER_KEY = os.getenv("DISCOGS_CONSUMER_KEY")
DISCOGS_CONSUMER_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET")
DISCOGS_API_URL = "https://api.discogs.com/database/search"

if not DISCOGS_CONSUMER_KEY or not DISCOGS_CONSUMER_SECRET:
    print("❌ Missing DISCOGS_CONSUMER_KEY or DISCOGS_CONSUMER_SECRET in .env.")
    sys.exit(1)

print(f"🔑 Using OAuth consumer key: {DISCOGS_CONSUMER_KEY[:6]}******")

params = {
    "q": "Inception",
    "type": "release",
    "per_page": 1,
    "key": DISCOGS_CONSUMER_KEY,
    "secret": DISCOGS_CONSUMER_SECRET,
}
headers = {"User-Agent": "UnguidedCapstoneAuthTest/1.0 +mark@example.com"}

print("🌐 Sending test request to Discogs API...")
response = requests.get(DISCOGS_API_URL, params=params, headers=headers, timeout=15)

print(f"📡 Status Code: {response.status_code}")

if response.status_code == 200:
    rate_limit = response.headers.get("X-Discogs-Ratelimit-Remaining", "unknown")
    print(f"✅ Auth Success! Rate limit remaining: {rate_limit}")
    data = response.json()
    if "results" in data and len(data["results"]) > 0:
        print(f"🎵 Example result title: {data['results'][0].get('title', 'N/A')}")
    else:
        print("⚠️ Request succeeded but no results found (still valid).")
elif response.status_code == 401:
    print("❌ Unauthorized. Invalid consumer key or secret.")
else:
    print(f"⚠️ Unexpected response: {response.status_code}")
    print(response.text)
