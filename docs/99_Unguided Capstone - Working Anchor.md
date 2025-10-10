### 🌅 **Morning Anchor — Discogs API Control**

**🪝 ANCHOR_STEP01_DISCogs_LIMIT_START**
 **Focus:**
 Move dataset size control (`MAX_DISCOG_TITLES`) upstream into **Step 01 (Acquire Discogs)** where API acquisition occurs.

**Current state:**

- Step 03 v4.7 is stable, reads local Discogs + TMDB JSONs correctly.
- Step 02 (TMDB) runs fine, waiting for a larger Discogs seed set.
- `MAX_DISCOG_TITLES = 200` already lives in `config.py`.
- Step 01 likely loops through candidate Discogs titles but lacks this limiter.

**Next action (AM):**
 Implement a Step 01 patch to:

1. Apply `MAX_DISCOG_TITLES` before API calls.
2. Log total titles requested / fetched.
3. Confirm output paths → `data/raw/discogs_raw/{plain,soundtrack}`.

Once that’s working, you’ll re-run Steps 01→02→03→04 to generate your full 200-movie Discogs ↔ TMDB dataset.