# Project: Surgical V2 Data Extraction Pipeline

## Project Overview
The goal is to build and optimize a "Surgical V2" data extraction pipeline to efficiently process a 420,000-page PDF corpus of lab reports into structured chemical analyte data using Vertex AI Batch and local heuristics.

## Active Constraints
- **Cost Efficiency:** The project is operating on a tight NGO budget. Extraction must use heavily minified Pydantic schemas to minimize Vertex AI LLM token costs. Pages sent to the LLM must be aggressively pre-filtered.
- **Environment:** GCP Environment MUST use Project: `open-ff-catalog-1` and Location: `us-central1`. Models should prioritize cost-effectiveness (e.g., `gemini-1.5-flash` instead of `pro`) where possible, utilizing Batch Pricing discounts.
- **Fuzzy Probing Limits:** Analyte fuzzy regex probing requires analytes to be >= 3 characters long and `MIN_MATCHES_PER_PAGE = 3` for a positive hit.

## Current State & Artifacts
- **Target Corpus:** 420,000 pages at `data/corpus/pdf_corpus.parquet`.
- **Project Organized:** Cleaned up `src/` directory. Legacy scripts moved to `src/legacy/` and tests moved to `src/tests/`.
- **Documentation:** `README.md` created with usage instructions; `journal.md` for daily activity logs.
- **Search Index:** SQLite FTS5 database at `data/corpus/corpus_search.db` supporting keyword, boolean, proximity, and fuzzy (trigram) searches across the full corpus.
- **V2 Pipeline:** Fully implemented and tested with 5% samples and a final full-run.
- **Legacy 26R Pipeline:** Updated `src/legacy/extract_26r_full.py` to extract expanded contact information (Last Name, First Name, Phone, Email) from Section A of Form 26R and include them in the HTML search frontend.

## Post-Completion Notes
The pipeline is now in a "Production-Ready" state for re-running on the full 26R corpus. 
- Ensure `GCP_PROJECT` and `GCP_LOCATION` are set in the environment.
- Use `src/estimate_batch_cost.py` to verify costs before any major run.
- Keep the `data/output/chunk_map.parquet` as it is the "brain" for re-associating micro-PDFs with original files.

