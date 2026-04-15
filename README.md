# Surgical V2 Data Extraction Pipeline

This repository contains the "Surgical V2" data extraction pipeline, designed to process large PDF corpora (e.g., lab reports) into structured chemical analyte data using Vertex AI Batch and local heuristics.

## Architecture Overview
The pipeline follows a "Sieve & Extract" architecture:
1.  **Phase 1: Local Triage:** Scans the corpus (420,000+ pages) using high-speed regex to identify high-value pages.
2.  **Phase 2: Physical Preparation:** Extracts target pages into 15-page "Micro-PDFs" for optimal LLM context.
3.  **Phase 3: LLM Extraction:** Uses Gemini 1.5 Flash in Batch mode with minified Pydantic schemas to minimize costs.
4.  **Phase 4: Data Reconstruction:** Harvests JSON results and joins them with original metadata.

---

## Getting Started

### 1. Preparation & Triage
*   `src/build_pdf_corpus.py`: Build the initial parquet corpus of text from PDFs.
*   `src/probe_corpus_fuzzy.py`: Run high-performance triage using positive/negative markers.
*   `src/triage_lab_reports.py`: Calculate heuristic scores for pages.
*   `src/create_triage_samples.py`: Generate visual samples for manual QC.

### 2. PDF Splitting
*   `src/split_pdfs.py`: Physically split the identified target pages into Micro-PDFs and generate the `chunk_map.parquet`.

### 3. Batch Execution
*   `src/estimate_batch_cost.py`: Run this first to project costs!
*   `src/prepare_batch_input_surgical_v2.py`: Generate the JSONL request file for Vertex AI.
*   `src/run_batch_job_surgical_v2.py`: Submit the job to Google Cloud.
*   `src/check_batch_status.py`: Monitor the job progress.

### 4. Harvesting & Cleaning
*   `src/harvest_batch_results_surgical_v2.py`: Download and parse results, re-joining metadata via the chunk map.
*   `src/clean_harvested_data.py`: Standardize units and formats.
*   `src/merge_form26r_metadata.py`: Join with facility-level metadata (Form 26R).
*   `src/summarize_harvested_data.py`: Generate extraction statistics.

---

## Core Components
*   `src/schemas_surgical.py`: Defines the "Lean" Pydantic models (`SurgicalSample`, `SurgicalResult`).
*   `src/get_single_file_prompt.py`: Utility to view the exact prompt sent to the LLM.
*   `src/run_single_file_extraction.py`: Test extraction on a single PDF.

## Legacy & Tests
*   `src/legacy/`: Contains previous versions (v1), generic experiments, and deprecated scripts.
*   `src/tests/`: Unit tests and validation scripts.
