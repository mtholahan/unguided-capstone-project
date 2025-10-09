"""
step_02_fetch_tmdb.py — Fetch TMDB metadata for Discogs titles
---------------------------------------------------------------
Refactored version with caching, rate limiting, and parallel fetch.
"""

import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from base_step import BaseStep
from config import (
    TMDB_API_URL,
    TMDB_SEARCH_URL,
    TMDB_DISCOVER_URL,
    TMDB_GENRE_URL,
    TMDB_API_KEY,
    DATA_DIR,
    MAX_THREADS,
    TMDB_RATE_LIMIT,     # new config-driven rate cap
    GOLDEN_TITLES,
    GOLDEN_TEST_MODE,
)
from utils import (
    cached_request,
    RateLimiter,
    make_progress_bar,
    save_json,
)


class Step02FetchTMDB(BaseStep):
    def __init__(self):
        super().__init__("Step02FetchTMDB")
        self.output_dir = DATA_DIR / "tmdb_raw"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limiter = RateLimiter(calls_per_second=3.0)

    # -------------------------------------------------------------
    def run(self):
        self.logger.info("🎬 Starting TMDB Fetch (Step 02)")
        start = time.time()

        if GOLDEN_TEST_MODE:
            results = self._fetch_golden(GOLDEN_TITLES)
        else:
            genre_map = self._fetch_genre_map()
            results = self._fetch_discover_movies(limit=500, genre_map=genre_map)

        # --- Save output ---
        df = pd.DataFrame(results)
        out_path = self.output_dir / "tmdb_results.csv"
        self.safe_overwrite(df, out_path)

        # --- Metrics ---
        duration = round(time.time() - start, 2)
        self.write_metrics({"records": len(df), "duration_sec": duration})
        self.logger.info(f"✅ Step 02 complete: {len(df):,} records in {duration:.2f}s")

    # -------------------------------------------------------------
    def _fetch_golden(self, titles):
        """Fetch TMDB entries for the Golden Titles set."""
        results = []
        for title in make_progress_bar(titles, desc="Golden Titles"):
            self.rate_limiter.wait()
            params = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US"}
            data = cached_request(f"{TMDB_SEARCH_URL}", params)
            if not data:
                continue
            items = data.get("results", [])
            expected_year = GOLDEN_EXPECTED_YEARS.get(title)
            chosen = self._select_best_match(items, expected_year)
            if chosen:
                results.append(chosen)
                save_json(chosen, self.output_dir / f"{title.replace(' ', '_')}.json")
        self.logger.info(f"🏆 Golden Mode complete: {len(results)} results.")
        return results

    # -------------------------------------------------------------
    def _fetch_discover_movies(self, limit, genre_map):
        """Fetch from TMDB Discover API with threading + rate limiting."""
        url = TMDB_DISCOVER_URL
        movies = []
        pages = range(1, int(limit / 20) + 2)

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for page in pages:
                params = {
                    "api_key": TMDB_API_KEY,
                    "page": page,
                    "sort_by": "popularity.desc",
                    "language": "en-US",
                    "include_adult": "false",
                }
                futures.append(executor.submit(self._fetch_page, url, params, genre_map))
            for f in make_progress_bar(as_completed(futures), desc="Discover Pages", total=len(futures)):
                data = f.result()
                if data:
                    movies.extend(data)
        return movies[:limit]

    # -------------------------------------------------------------
    def _fetch_page(self, url, params, genre_map):
        """Fetch a single TMDB discover page."""
        self.rate_limiter.wait()
        data = cached_request(url, params)
        results = []
        for m in (data or {}).get("results", []):
            year = None
            if m.get("release_date"):
                try:
                    year = int(m["release_date"].split("-")[0])
                except Exception:
                    pass
            genres = "|".join(genre_map.get(g, "") for g in m.get("genre_ids", []))
            results.append({
                "tmdb_id": m.get("id"),
                "title": m.get("title"),
                "release_year": year,
                "genres": genres,
            })
        return results

    # -------------------------------------------------------------
    def _fetch_genre_map(self):
        """Fetch genre map with caching."""
        data = cached_request(f"{TMDB_GENRE_URL}", {"api_key": TMDB_API_KEY, "language": "en-US"})
        genres = data.get("genres", []) if data else []
        return {g["id"]: g["name"] for g in genres}

    # -------------------------------------------------------------
    def _select_best_match(self, items, expected_year):
        """Pick the best TMDB result by expected year."""
        if not items:
            return None
        if expected_year:
            for item in items:
                try:
                    year = int(item.get("release_date", "0000").split("-")[0])
                    if year == expected_year:
                        return item
                except Exception:
                    continue
        return items[0]
