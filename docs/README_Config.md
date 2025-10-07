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