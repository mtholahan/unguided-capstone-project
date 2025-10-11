# 🎬 Capstone: Linking Soundtrack Genres to Movie Popularity

**Subtitle:** Discogs + TMDb Data Engineering Pipeline

------

## Introduction

- **Goal:** Explore how soundtrack genres (Discogs) relate to movie popularity (TMDb).
- **Challenge:** Cross-domain entity matching between music releases and film metadata.
- **Approach:** End-to-end ETL pipeline integrating Discogs and TMDb, verified locally and prepared for Azure scaling.

------

## Step 4 – Data Exploration & Enrichment

- Cleaned and normalized Discogs + TMDb datasets.
- Implemented `normalize_title()` for punctuation, case, and Unicode consistency.
- Removed non-film or compilation releases; deduplicated by artist + year + title.
- Enriched Discogs records with TMDb metadata and genres via API calls.
- Used RapidFuzz for Discogs–TMDb title matching with year and substring filters.
- Produced `data/intermediate/discogs_tmdb_matches.csv` as the validated join output.

------

## Step 5 – Prototype Pipeline & Validation

- Refactored all pipeline logic into modular scripts under `/scripts/`.
- Implemented dynamic path resolution and configuration-based loading (`config.json`).
- Created a portable Jupyter notebook (`evidence/validation.ipynb`) to verify reproducibility.
- Verified end-to-end execution and match quality (≈ 80 % of pairs ≥ 0.8 similarity score).
- Exported `validation_summary.csv` for audit evidence.

**Outcome:**
 ✅ Clean, modular, and portable pipeline validated for mentor review.
 ✅ Ready for cloud scaling using PySpark and Azure services.

------

## Step 6 – Scaling the Prototype

- Migrate ETL logic to **PySpark** for distributed processing.
- Store input/output artifacts in **Azure Blob Storage**.
- Execute Spark jobs via Databricks or Azure HDInsight.
- Demonstrate linear scaling on expanded Discogs dataset vs. local subset.

------

## Step 7 – Deployment Architecture

- Azure components: Blob Storage, Spark Cluster, and Log Analytics.
- Data flow: Discogs dump → Spark processing → Blob output → Dashboard.
- Design trade-offs: simplicity and transparency over cost optimization.
- Export ARM templates for reproducibility and automated setup.

------

## Step 8 – Deploy for Testing

- Add `tests/` folder with pytest suite (title cleaning, year parsing, fuzzy matching).
- Deploy pipeline code to Azure compute.
- Record test coverage and edge cases (“Amadeus,” “Whiplash,” “Interstellar”).

------

## Step 9 – Production Deployment

- Deploy optimized pipeline to full Azure architecture.
- Process complete Discogs dataset + TMDb enrichment.
- Output Parquet files to Blob Storage.
- Implement cost controls and automated resource shutdown.

------

## Step 10 – Monitoring Dashboard

- Build Azure Monitor + Log Analytics dashboard.
- Track metrics: Spark job status, compute utilization, storage growth, and pipeline errors.
- Ensure pipeline reliability and cost efficiency.

------

## Step 11 – Final Submission Package

- GitHub Repo: `scripts/`, `evidence/`, `spark/`, `docs/`.
- README: datasets, pipeline steps, ERD, and deployment diagram.
- Slide Deck: updated story from Discogs exploration to Azure deployment.
- Dashboard: screenshots and metrics summary included.

------

## Results & Insights

- Achieved ≈ 80 % Discogs–TMDb match rate (≥ 0.8 score).
- Identified soundtrack genre trends by decade and popularity.
- Correlated film success with genre diversity and style patterns.
- Limitations: partial OST coverage, metadata gaps, composer ambiguity.

------

## Conclusion & Future Work

- Demonstrated end-to-end pipeline from Discogs + TMDb to Azure deployment.
- Validated reproducibility and scalability of the unguided ETL framework.
- Future enhancements: composer linkage, multilingual matching, real-time updates.
- **Key Takeaway:** Reliable entity resolution between music and film domains can unlock rich cross-media insights.

------

## 📸 Must-Have Visuals for Slide Deck

### Step 5 – Prototype Pipeline

-  Pipeline flow diagram (`scripts/` orchestration).
-  Screenshot of validation notebook (summary and score distribution).

### Step 6 – Scaling

-  Screenshot of Azure Blob container with Discogs input/output.
-  Screenshot of Databricks/Spark cluster running job.

### Step 7 – Deployment Architecture

-  Diagram showing Blob → Spark → Blob → Dashboard flow.
-  Screenshot of Azure resources in portal.

### Step 8 – Testing

-  Pytest results summary and edge case diagnostics.

### Step 9 – Production Deployment

-  Screenshot of Parquet output in Azure Blob.
-  Flow diagram: dev → test → prod.

### Step 10 – Monitoring Dashboard

-  Screenshot of Azure Monitor/Log Analytics dashboard.
-  Key metrics panel (utilization, storage, errors).

### Step 11 – Final Submission

-  GitHub repo structure (`scripts/`, `evidence/`, `spark/`, `docs/`).
-  README excerpt (showing datasets, pipeline steps, limitations).
-  ERD diagram (Discogs ↔ TMDb).
-  Final Azure architecture diagram.

### Results & Insights

-  Chart: soundtrack genre frequency by decade.
-  Chart: TMDb popularity vs. soundtrack genre correlation.