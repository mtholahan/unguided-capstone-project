"""Step 06: Fetch TMDb (Refactored)
-----------------------------------
Fetches movies from TMDb via the Discover or Search API.

Modes:
- Golden Test Mode (uses configured iconic titles)
- Regular Discover Mode (up to ROW_LIMIT or 10,000 movies)

Outputs:
- TMDB_DIR/enriched_top_1000.csv
- TMDB_DIR/Step06_Documentation.txt
- metrics/step06_fetch_tmdb.json
"""

from base_step import BaseStep
import requests
import pandas as pd
import time
from config import (
    TMDB_DIR, TMDB_API_KEY, ROW_LIMIT,
    TMDB_DISCOVER_URL, TMDB_SEARCH_URL, TMDB_GENRE_URL,
    GOLDEN_TITLES, GOLDEN_EXPECTED_YEARS, GOLDEN_TEST_MODE
)


class Step06FetchTMDb(BaseStep):
    def __init__(self, name="Step 06: Fetch TMDb Discover"):
        super().__init__(name=name)
        self.api_key = TMDB_API_KEY
        self.output_path = TMDB_DIR / "enriched_top_1000.csv"
        self.max_movies = 1000  # overridden by ROW_LIMIT or Golden Mode
        self.max_pages = 500
        self.session = requests.Session()

    # -------------------------------------------------------------
    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 06: Fetch TMDb (Refactored)")

        if not self.api_key:
            self.fail("TMDB_API_KEY not set in environment or config.")
            return

        if GOLDEN_TEST_MODE:
            self.logger.info(f"🌟 Golden Test Mode active: fetching {len(GOLDEN_TITLES)} iconic movies.")
            movies = self._fetch_golden(GOLDEN_TITLES)
        else:
            effective_limit = min(ROW_LIMIT or self.max_movies, 10_000)
            self.logger.info(f"▶ Fetching up to {effective_limit:,} TMDb movies (Discover API)...")
            genre_map = self._fetch_genre_map()
            movies = self._fetch_discover_movies(effective_limit, genre_map)

        # --- Write output CSV ---
        df = pd.DataFrame(movies)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)
        self.logger.info(f"✅ Wrote {len(df):,} rows → {self.output_path.name}")

        # --- Write documentation ---
        doc_text = f"""
Step 06 Output Documentation
============================
File: {self.output_path.name}
Schema:
  tmdb_id, title, release_year, genres

Modes:
  Golden Test Mode: {GOLDEN_TEST_MODE}
  ROW_LIMIT: {ROW_LIMIT or '∞'}
  Default max_movies: {self.max_movies}

Notes:
  - Discover endpoint limited to {self.max_pages} pages (~10,000 movies).
  - Golden Test Mode overrides ROW_LIMIT.
"""
        doc_path = TMDB_DIR / "Step06_Documentation.txt"
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(doc_text)
        self.logger.info(f"📝 Wrote documentation → {doc_path.name}")

        # --- Metrics ---
        metrics = {
            "rows_total": len(df),
            "mode": "golden" if GOLDEN_TEST_MODE else "discover",
            "row_limit_active": bool(ROW_LIMIT),
            "api_key_present": bool(self.api_key),
        }
        self.write_metrics("step06_fetch_tmdb", metrics)
        self.logger.info(f"📊 Metrics recorded: {metrics}")
        self.logger.info("✅ [DONE] Step 06 completed successfully.")

    # -------------------------------------------------------------
    def _fetch_discover_movies(self, limit: int, genre_map: dict) -> list[dict]:
        """Fetch popular movies using the TMDb Discover API."""
        url = TMDB_DISCOVER_URL
        movies, page, per_page = [], 1, 20

        for _ in self.progress_iter(range(limit), desc="Fetching TMDb Discover"):
            if len(movies) >= limit or page > self.max_pages:
                break

            params = {
                "api_key": self.api_key,
                "page": page,
                "sort_by": "popularity.desc",
                "language": "en-US",
                "include_adult": "false",
            }

            try:
                r = self._safe_get(url, params)
                data = r.json()
            except Exception as e:
                self.logger.error(f"❌ Error fetching page {page}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for m in results:
                if len(movies) >= limit:
                    break
                tmdb_id = m.get("id")
                title = m.get("title", "")
                rd = m.get("release_date") or ""
                try:
                    year = int(rd.split("-")[0])
                except Exception:
                    year = None
                genre_ids = m.get("genre_ids", [])
                genres = "|".join(genre_map.get(gid, "") for gid in genre_ids)
                movies.append({
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "release_year": year,
                    "genres": genres
                })

            page += 1

        self.logger.info(f"🎬 Completed TMDb fetch: {len(movies):,} movies collected.")
        return movies

    # -------------------------------------------------------------
    def _fetch_golden(self, titles: set[str]) -> list[dict]:
        """Fetch Golden Test Mode movies by title search."""
        url = TMDB_SEARCH_URL
        movies = []

        for title in self.progress_iter(titles, desc="Fetching Golden Set"):
            try:
                r = self._safe_get(url, {"api_key": self.api_key, "query": title, "language": "en-US"})
                results = r.json().get("results", [])
            except Exception as e:
                self.logger.error(f"❌ TMDb error for {title}: {e}")
                continue

            if not results:
                self.logger.warning(f"⚠️ No TMDb result for golden title: {title}")
                continue

            expected_year = GOLDEN_EXPECTED_YEARS.get(title)
            chosen = results[0]

            if expected_year:
                for cand in results:
                    rd = cand.get("release_date") or ""
                    try:
                        year = int(rd.split("-")[0])
                    except Exception:
                        year = None
                    if year == expected_year:
                        chosen = cand
                        break

            rd = chosen.get("release_date") or ""
            try:
                year = int(rd.split("-")[0])
            except Exception:
                year = None
            genres = "|".join(str(gid) for gid in chosen.get("genre_ids", []))

            movies.append({
                "tmdb_id": chosen.get("id"),
                "title": chosen.get("title", ""),
                "release_year": year,
                "genres": genres
            })

        self.logger.info(f"🏆 Golden Mode fetch complete: {len(movies):,} titles.")
        return movies

    # -------------------------------------------------------------
    def _fetch_genre_map(self) -> dict[int, str]:
        """Fetch genre ID → name map."""
        url = TMDB_GENRE_URL
        try:
            r = self._safe_get(url, {"api_key": self.api_key, "language": "en-US"})
            data = r.json().get("genres", [])
            genre_map = {g["id"]: g["name"] for g in data}
            self.logger.info(f"🎭 Loaded {len(genre_map):,} TMDb genres.")
            return genre_map
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch genre map: {e}")
            return {}

    # -------------------------------------------------------------
    def _safe_get(self, url: str, params: dict, retries: int = 3, backoff: float = 2.0):
        """Simple retry wrapper for TMDb API calls."""
        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=10)
                r.raise_for_status()
                return r
            except Exception as e:
                if attempt == retries:
                    raise
                wait = backoff * attempt
                self.logger.warning(f"⚠️ TMDb request failed (attempt {attempt}/{retries}): {e} → retrying in {wait:.1f}s")
                time.sleep(wait)
        return None


if __name__ == "__main__":
    Step06FetchTMDb().run()
