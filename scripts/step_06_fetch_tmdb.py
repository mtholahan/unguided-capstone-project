# scripts/step_06_fetch_tmdb.py

from base_step import BaseStep
import os
import requests
import pandas as pd
from pathlib import Path
from config import TMDB_DIR, TMDB_API_KEY

class Step06FetchTMDb(BaseStep):
    def __init__(self, name="Step 06: Fetch TMDb Top 1000"):
        super().__init__(name)
        self.api_key = TMDB_API_KEY
        self.output_path = TMDB_DIR / "enriched_top_1000.csv"
        self.max_movies = 1000

    def run(self):
        if not self.api_key:
            raise RuntimeError("TMDB_API_KEY not set")
        self.logger.info("▶ Fetching top 1,000 TMDb movies…")
        genre_map = self._fetch_genre_map()
        movies = []
        page = 1
        while len(movies) < self.max_movies:
            data = self._fetch_page(page)
            results = data.get("results", [])
            if not results:
                break
            for m in results:
                tmdb_id = m.get("id")
                title   = m.get("title", "")
                rd      = m.get("release_date") or ""
                try:
                    year = int(rd.split("-")[0])
                except:
                    year = None
                genre_ids = m.get("genre_ids", [])
                genres = "|".join(genre_map.get(gid, "") for gid in genre_ids)
                movies.append({
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "release_year": year,
                    "genres": genres
                })
                if len(movies) >= self.max_movies:
                    break
            self.logger.info(f"   • Page {page}, collected {len(movies)} total")
            page += 1
            if page > data.get("total_pages", 1):
                break

        df = pd.DataFrame(movies)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)
        self.logger.info(f"✅ Wrote {len(df)} rows to {self.output_path}")

    def _fetch_page(self, page: int) -> dict:
        url = "https://api.themoviedb.org/3/movie/top_rated"
        r = requests.get(url, params={"api_key": self.api_key, "page": page, "language": "en-US"})
        r.raise_for_status()
        return r.json()

    def _fetch_genre_map(self) -> dict[int, str]:
        url = "https://api.themoviedb.org/3/genre/movie/list"
        r = requests.get(url, params={"api_key": self.api_key, "language": "en-US"})
        r.raise_for_status()
        data = r.json().get("genres", [])
        return {g["id"]: g["name"] for g in data}

