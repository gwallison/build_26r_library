# Project: Surgical V2 Data Extraction Pipeline

## Project Overview
The goal is to build and optimize a "Surgical V2" data extraction pipeline to efficiently process a 420,000-page PDF corpus of lab reports into structured chemical analyte data using Vertex AI Batch and local heuristics.

## Active Constraints
- **Cost Efficiency:** The project is operating on a tight NGO budget. Extraction must use heavily minified Pydantic schemas to minimize Vertex AI LLM token costs. Pages sent to the LLM must be aggressively pre-filtered.
- **Environment:** GCP Environment MUST use Project: `open-ff-catalog-1` and Location: `us-central1`. Models should prioritize cost-effectiveness (e.g., `gemini-1.5-flash` instead of `pro`) where possible, utilizing Batch Pricing discounts.
- **Fuzzy Probing Limits:** Analyte fuzzy regex probing requires analytes to be >= 3 characters long and `MIN_MATCHES_PER_PAGE = 3` for a positive hit.

## Current State & Artifacts
- **Target Corpus:** 420,000 pages at `data/corpus/pdf_corpus.parquet`.
- **Schemas (`schemas_surgical.py`):** Models `SurgicalSample`, `SurgicalResult`, and `SurgicalExtraction`. Keys are minified (e.g., `sid` for lab_sample_id) to save LLM tokens.
- **Batch Processing:** `harvest_batch_results_surgical_v2.py` parses chunked Vertex AI Batch results and restores metadata via proximity joins.
- **Triage Scripts:** 
  - `triage_lab_reports.py`: Heuristic scoring for lab reports.
  - `probe_corpus_fuzzy.py`: Probes the corpus using a regex pattern built from analytes to pre-filter pages. Currently suffering from severe performance issues (hanging) due to regex complexity on large lists.

## Planned Work: Optimization of Fuzzy Triage
To resolve the performance issues and save LLM costs by excluding Lab Control pages (Method Blanks, LCS, Spikes), the following steps are planned:

### 1. Curate Core "Indicator" Lists (Manual & Automated)
To fix the regex hang and improve filtering precision, transition from massive, unoptimized lists to focused, curated lists.
- **Action 1 (Positive Analytes):** Review the current analyte list (`data/output/core_analytes_review.csv`) and trim it down to the top 50-100 most common/reliable "indicator" analytes (e.g., Benzene, Arsenic, TDS). Save this smaller list as `data/output/core_analytes_trimmed.csv`.
- **Action 2 (Lab Control Terms):** Create a script to sample a subset of the corpus and extract common terms indicative of Lab Control sections (e.g., "Method Blank", "LCS", "Matrix Spike", "Surrogate"). Curate this output into a definitive `data/output/lab_control_markers.csv` list.

### 2. Update `probe_corpus_fuzzy.py` for High Performance and Dual-Filtering
Rewrite the probing logic to use standard, non-capturing regex or efficient string searching to prevent hangs, and implement both positive and negative markers.
- **Action:** Update the script to load `core_analytes_trimmed.csv`, the curated `lab_control_markers.csv`, and a hardcoded list of `RE_ANALYTICAL_MARKERS` (e.g., "Client Sample ID", "Date Collected").
- **Action:** Optimize the regex pattern to use non-capturing groups `(?:...)` to prevent hanging, or utilize an iterative multi-pattern string search.
- **Action:** Implement "Positive Filtering" to require explicit analytical markers on the page.
- **Action:** Implement "Negative/Scrutiny Filtering": If a page hits the lab control markers heavily, flag it differently (e.g., `is_lab_control = True`) so it can be scrutinized separately rather than sent directly to the expensive LLM batch.

### 3. Optimize Batch Processing Costs
Once the triage is running fast and accurately filtering the corpus, review the batch execution scripts (`run_batch_job_surgical_v2.py`).
- **Action:** Ensure `gemini-1.5-flash` is used instead of `gemini-1.5-pro` (Flash is significantly cheaper and usually sufficient for structured extraction).
- **Action:** Review the prompt size and ensure Vertex AI Batch Pricing discounts are fully applied.
