## 🧩 `config.py` Overview

**Unguided Capstone — MusicBrainz × TMDb Data Pipeline**
 *Last updated: Oct 2025*

`config.py` centralizes all constants, paths, and external service settings used by the ETL pipeline.
 It replaces “magic numbers” and scattered literals across Step 00 – 06 scripts, improving portability, testing, and mentor traceability.

------

### ⚙️ **1. Imports & Environment Setup**

Handles `.env` loading via `python-dotenv` and exposes:

- `print_config_summary()` → prints uppercase constants at runtime for debugging.

------

### 🗂️ **2. Core Paths**

Defines your local project layout and staging structure:

| Variable                                    | Example                                  | Description             |
| ------------------------------------------- | ---------------------------------------- | ----------------------- |
| `BASE_DIR`                                  | `D:/Capstone_Staging`                    | Root for all local data |
| `DATA_DIR`, `RESULTS_DIR`, `METRICS_DIR`    | Subdirectories for raw/processed/metrics |                         |
| `MB_RAW_DIR`, `MB_CLEANSED_DIR`, `TMDB_DIR` | Domain-specific data folders             |                         |
| `SCRIPTS_PATH`, `SEVEN_ZIP_PATH`            | Tooling and code paths                   |                         |

------

### 🗃️ **3. Database Settings**

Connection parameters for the local Postgres instance:
 `PG_HOST`, `PG_PORT`, `PG_DBNAME`, `PG_USER`, `PG_PASSWORD`, `PG_SCHEMA`.

------

### 🚀 **4. Performance, Toggles & Thresholds**

Central location for runtime tuning and debugging flags:

- `CHUNK_SIZE`, `ROW_LIMIT`, `AUDIT_SAMPLE_LIMIT` – batch & sample limits
- `SLEEP_SECONDS` – API throttling
- `FUZZY_THRESHOLD`, `YEAR_TOLERANCE`, `MAX_CANDIDATES_PER_TITLE` – matching controls
- `GOLDEN_TEST_MODE`, `GOLDEN_TEST_SIZE` – benchmark/testing flags
- `STEP_METRICS` – runtime metrics store

## 🪜 Phase 1 Magic Number Migration Table

| Constant Name             | Default Value | Source Script(s)                                             | Purpose / Usage                              | Notes                                       |
| ------------------------- | ------------- | ------------------------------------------------------------ | -------------------------------------------- | ------------------------------------------- |
| `DEFAULT_RETRY_COUNT`     | `2`           | `main.py`                                                    | Retry limit for resumable steps              | Applies to lightweight loops and retries    |
| `RETRY_DELAY_SECONDS`     | `5`           | `main.py`                                                    | Delay between retry attempts                 | Adjustable per environment                  |
| `DOWNLOAD_BUFFER_SIZE`    | `1024`        | `step_00_acquire_musicbrainz.py`                             | Chunk size for streamed file downloads       | Matches MB dump streaming chunk size        |
| `MAX_RETRY_ATTEMPTS`      | `10`          | `step_00_acquire_musicbrainz.py`                             | Download retry ceiling                       | Used for MusicBrainz acquisition robustness |
| `GUID_SAMPLE_LIMIT`       | `6`           | `step_03b_rehydrate_guids.py`                                | Sample size for GUID rehydration test output | Used in local validation prints             |
| `GUID_RETRY_LIMIT`        | `40`          | `step_03b_rehydrate_guids.py`                                | Max allowed missing GUIDs before halt        | Runtime validation                          |
| `JOIN_SAMPLE_LIMIT`       | `100`         | `step_03b_rehydrate_guids.py`, `step_04_mb_full_join.py`     | Row limit for join audits                    | Prevents large-memory test sets             |
| `JOIN_ITERATION_LIMIT`    | `5`           | `step_04_mb_full_join.py`                                    | Max join retry attempts                      | For partial join fallback                   |
| `JOIN_TOLERANCE`          | `0.001`       | `step_04_mb_full_join.py`                                    | Float precision tolerance for join scoring   | Replace inline constants                    |
| `FILTER_THRESHOLD`        | `0.02`        | `step_05_filter_soundtracks_enhanced.py`                     | Minimum match score for soundtrack inclusion | Data refinement control                     |
| `FILTER_SAMPLE_SIZE`      | `42`          | `step_05_filter_soundtracks_enhanced.py`                     | Random sample size for debugging subsets     | For diagnostic previews                     |
| `SOUNDTRACK_SUBSET_LIMIT` | `100`         | `step_05_filter_soundtracks_enhanced.py`                     | Max rows for subset export                   | Used in partial parquet outputs             |
| `TMDB_RESULT_LIMIT`       | `1000`        | `step_06_fetch_tmdb.py`                                      | Number of candidate titles to query          | Reduces API load                            |
| `TMDB_PAGE_SIZE`          | `500`         | `step_06_fetch_tmdb.py`                                      | Max results per TMDb page                    | Dependent on TMDb API                       |
| `TMDB_RETRY_DELAY`        | `20`          | `step_06_fetch_tmdb.py`                                      | Pause between TMDb API calls                 | Respect rate limit                          |
| `TMDB_TOTAL_LIMIT`        | `10000`       | `step_06_fetch_tmdb.py`                                      | Hard stop for cumulative API fetches         | Guardrail for runaway loops                 |
| `AUDIT_SAMPLE_LIMIT`      | `30`          | `step_01_audit_raw.py`, `step_03_util_check_tsv_structure.py` | Sample size for audit reports                | Already partially implemented               |
| `CLEANSE_SAMPLE_LIMIT`    | `20`          | `step_02_cleanse_tsv.py`                                     | Max sample size for cleaning checks          | Avoids full TSV scans                       |



------

### 🌐 **5. External Services**

Consolidates:

- `TMDB_API_KEY` (from `.env`)
- API URLs for MusicBrainz & TMDb (`MB_DUMP_URL`, `TMDB_DISCOVER_URL`, etc.)
- Azure placeholders (`AZURE_CONN_STR`, `BLOB_CONTAINER`)

------

### 🎞️ **6. Data File Mappings**

Organized by data domain:

- **MusicBrainz:** whitelist, raw/cleansed TSVs, derived files (`release_enriched.tsv`, `soundtracks.parquet`, etc.)
- **TMDb:** local enriched/genre CSVs and junk-title lists
- **Output + Refresh Maps:** consolidated in `MATCH_OUTPUTS`, `REFRESH_INPUTS`, `MB_STATIC_REFRESH`.

------

### 🏆 **7. Reference Data**

Static validation sets for mentor checks & data sanity:

- `GOLDEN_TITLES` → canonical blockbuster list
- `GOLDEN_EXPECTED_YEARS` → disambiguation mapping for fuzzy joins.

------

### 🧱 **8. Config Class**

Lightweight wrapper for step scripts using `self.config`.
 Provides safe, object-style access to core paths and credentials for `BaseStep` inheritance.

------

### 💡 **Extending the Config**

When adding new constants:

1. **Add →** in the correct section (prefer upper snake case).
2. **Import →** via `from config import <CONSTANT>` in scripts.
3. **Test →** run `print_config_summary()` or your pipeline entrypoint.
4. **Commit →** with a clear “config update” message.