# 🎬 Capstone: Linking Soundtrack Genres to Movie Popularity
Subtitle: MusicBrainz + TMDb Pipeline

---

## Introduction
- Goal: Explore relationship between movie popularity (TMDb) and soundtrack genres (MusicBrainz)  
- Challenge: Cross-domain join (music albums vs. films)  
- Approach: ETL pipeline, fuzzy matching, scaling to Azure  

---

## Step 4 Recap – Data Exploration & Enrichment
- Cleaned & normalized MB + TMDb datasets  
- Implemented `clean_title()` + UTF-8 normalization  
- Fuzzy matching with RapidFuzz + year filters  
- Built ERD schema (MB release_group ↔ TMDb movie/genres)  

---

## Step 5 – Prototype Pipeline
- Wrapped 11 scripts into single driver (`main.py`)  
- End-to-end ETL: acquire → clean → join → load  
- Implemented logging (row counts, errors, runtime)  
- Snapshot: local run on 2–3 GB MB dataset  

---

## Step 6 – Scaling the Prototype
- Migrated core logic to PySpark (cleaning + joins)  
- Stored input/output in Azure Blob Storage  
- Deployed Spark cluster via Databricks  
- Performance gain: handled entire MB dump vs. subset  

---

## Step 7 – Deployment Architecture
- Azure components: Blob Storage, Spark Cluster, Log Analytics  
- Data flow: MB dump → Spark processing → Blob output → Dashboard  
- Tradeoffs: simplicity > cost optimization (small cluster chosen)  
- Exported ARM templates for reproducibility  

---

## Step 8 – Deploy for Testing
- Added `tests/` folder with pytest suite (title cleaning, year parsing, fuzzy matching)  
- Deployed pipeline code to Azure compute  
- Results: [X tests passed, Y% coverage]  
- Edge cases highlighted: Amadeus, Interstellar, Whiplash  

---

## Step 9 – Production Deployment
- Deployed optimized pipeline to full Azure architecture  
- Processed complete MB dataset + TMDb enrichment  
- Output written to Blob (CSV/Parquet)  
- Implemented cost control: deleted idle clusters  

---

## Step 10 – Monitoring Dashboard
- Built dashboard using Azure Monitor + Log Analytics  
- Metrics tracked:  
  - Compute utilization (Spark jobs)  
  - Storage usage (Blob growth)  
  - Pipeline job status/errors  
- Ensures pipeline reliability & cost efficiency  

---

## Step 11 – Final Submission Package
- GitHub Repo: `src/`, `tests/`, `spark/`, `docs/`  
- README: datasets, pipeline steps, ERD, deployment diagram  
- Slide Deck: incremental story from Step 1 → Step 11  
- Dashboard: deployed & accessible (or screenshot provided)  

---

## Results & Insights
- Achieved ~40% MB↔TMDb match rate (1,400+ high-confidence pairs)  
- Identified soundtrack genre trends by decade  
- Observed correlations between popularity & soundtrack type  
- Limitations: incomplete OST coverage, noisy joins, composer mismatch  

---

## Conclusion & Future Work
- Demonstrated end-to-end pipeline from raw MB dump + TMDb API → deployed Azure solution  
- Proved feasibility of linking soundtrack genres to movie popularity  
- Future improvements: curated OST list, composer/director signals, multilingual handling  
- Key takeaway: even imperfect entity resolution can yield meaningful exploratory insights  



# 📸 Must-Have Visuals for Slide Deck

## Step 5 – Prototype Pipeline
- [ ] Flow diagram of pipeline (Python scripts orchestration)  
- [ ] Screenshot of pipeline log (row counts, warnings, runtime)  

## Step 6 – Scaling
- [ ] Screenshot of Azure Blob container with MB/processed files  
- [ ] Screenshot of Databricks or Spark cluster running job  

## Step 7 – Deployment Architecture
- [ ] Architecture diagram (Blob → Spark → Blob → Dashboard)  
- [ ] Screenshot of Azure resources deployed (portal view)  

## Step 8 – Testing
- [ ] Pytest results in terminal (passed/failed, coverage %)  
- [ ] Edge case output (e.g., “Interstellar missing” diagnostic)  

## Step 9 – Production Deployment
- [ ] Screenshot of full dataset output in Azure Blob (CSV/Parquet)  
- [ ] Flow diagram/test-to-prod comparison  

## Step 10 – Monitoring Dashboard
- [ ] Screenshot of Azure Monitor/Log Analytics dashboard  
- [ ] Key metrics view (compute, storage, job status)  

## Step 11 – Final Submission
- [ ] Screenshot of GitHub repo structure (`src/`, `tests/`, `spark/`, `docs/`)  
- [ ] README excerpt showing datasets, pipeline steps, limitations  
- [ ] ERD diagram (from Step 4, reused)  
- [ ] Final Azure architecture diagram  

## Results & Insights
- [ ] Chart: soundtrack genre frequency by decade  
- [ ] Chart: TMDb popularity vs. soundtrack genre correlation  