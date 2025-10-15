import requests
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_USER_AGENT = "UnguidedCapstoneBot/1.0"
DISCOGS_TOKEN = "xRRUMomDjPHLjJLmSBgkiAFqcPfZbNWpwzLfbnLD"

params = {
    "q": "Blade Runner",
    "type": "release",
    "token": DISCOGS_TOKEN,   # must be lowercase
}

r = requests.get(
    DISCOGS_API_URL,
    params=params,
    headers={"User-Agent": DISCOGS_USER_AGENT},
)

print("HTTP:", r.status_code)
print("Text:", r.text[:300])
