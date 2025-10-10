Resume from anchor `UNG_S5_DISCogsOAuth_2025-10-10`

1. ### 🧩 **Anchor: Unguided Step 5 – Discogs→TMDB Prototype (Auth & Stability Layer)**

   **Date:** 2025-10-10
    **Status:** Active development / final pre-submission prep

   #### 🔹 Current State

   - Working on **Unguided Capstone Step 5** (Prototype Data Pipeline).
   - Guided Capstone Step 2 paused.
   - Main architecture: **Discogs → TMDB** pipeline (Steps 01-04 orchestrated in `main.py`).

   #### 🔹 Key Files

   - `step_01_acquire_discogs.py` → actively refactored

   - `utils.py` → using `cached_request()` with Discogs OAuth key + secret

   - `config.py` → defines

     ```
     DISCOGS_CONSUMER_KEY = os.getenv("DISCOGS_CONSUMER_KEY")
     DISCOGS_CONSUMER_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET")
     ```

   - `.env` → contains working consumer key/secret

   #### 🔹 Latest Additions

   - Cleaned `fetch_discogs_for_title()` (title normalization + rate-limit handler)
   - Updated `cached_request()` for OAuth-param auth (key/secret)
   - Built `scripts/test_discogs_auth.py` (confirmed Discogs 200 OK)

   #### 🔹 Outstanding Items

   1. Confirm Step 01 runs at scale (e.g., `DISCOG_MAX_TITLES = 200`).
   2. Validate pipeline orchestration in `main.py` (Steps 1-4 only).
   3. Generate Step 5 submission artifacts once Discogs acquisition is stable.

   