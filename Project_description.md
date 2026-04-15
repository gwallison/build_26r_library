# Project Overview: Surgical V2 Data Extraction Pipeline

## Executive Summary
This project aims to convert a massive, heterogeneous corpus of **420,000 PDF pages** (Pennsylvania laboratory reports) into a structured, searchable database of chemical analytes. Operating on a strict NGO budget, the pipeline utilizes **Large Language Models (LLMs)** via **Vertex AI Batch Processing** to perform "surgical" data extraction with high precision and minimal cost.

The primary challenge is the scale: processing 420,000 pages through standard LLM APIs would cost thousands of dollars. Our solution uses a tiered "Sieve & Extract" architecture that reduces the workload by **~80%** before any data is sent to the paid LLM.

---

## The Architecture: A Tiered Pipeline

The pipeline is divided into three distinct phases: **Triage (Local)**, **Preparation (Local)**, and **Extraction (Cloud)**.

### Phase 1: Local Triage (The "Digital Sieve")
To keep costs low, we perform aggressive filtering on your local machine before engaging the LLM.

*   **Analyte Probing:** We use high-speed regular expressions (Regex) to scan the raw text of all 420,000 pages. A page is only "flagged" if it contains at least **3 unique chemical indicators** (e.g., Benzene, Arsenic, TDS).
*   **Intelligent Categorization:** Lab reports are messy. Data tables often share pages with Quality Control (QC) data. We categorize every flagged page into one of four buckets:
    1.  **Golden:** High-confidence pages containing both analytes and sample metadata (IDs, dates).
    2.  **Mixed:** Pages containing target data alongside "Lab Control" markers (Method Blanks, Spikes).
    3.  **Continuation:** Pages that are clearly part of a table but lack headers (overflow pages).
    4.  **Pure QC:** Pages containing only control data (Excluded to save cost).
*   **Performance:** Our optimized triage script can "read" the entire 420,000-page corpus in approximately **4.5 minutes** on a standard workstation.

### Phase 2: Physical Preparation (Micro-PDFs)
LLMs perform best when they have a focused "context window." Sending a 500-page PDF to an LLM is expensive and prone to errors.

*   **Physical Splitting:** We extract only the specific high-value pages identified in Phase 1 and group them into **15-page "Micro-PDFs"**.
*   **The Chunk Map:** Because we are physically moving pages around, we maintain a "brain" file (`chunk_map.parquet`) that remembers exactly where every extracted page came from in the original 420,000-page library.

### Phase 3: The LLM Engine (Vertex AI Batch)
We utilize Google’s **Gemini 1.5 Flash** model for the actual extraction.

*   **Why Gemini Flash?** It is designed for high-speed, structured extraction tasks. It is significantly cheaper than the "Pro" versions while maintaining high accuracy for tabular data.
*   **Batch Pricing Strategy:** By using "Batch" mode instead of real-time API calls, we receive a **50% flat discount** from Google.
*   **Surgical Schema:** We use a "lean" Pydantic schema with minified keys (e.g., `sid` instead of `lab_sample_id`). This reduces the number of "tokens" (the currency of LLMs) we pay for in every request.

---

## Cost Guardrails & Quality Control

To prevent "sticker shock," the pipeline includes built-in financial protections:

1.  **The Cost Estimator:** Before any job is submitted to the cloud, a local script analyzes the input files and provides a line-item estimate of the cost (Input vs. Output tokens).
2.  **5% Sample Testing:** We never submit the full corpus at once. The pipeline supports a `--sample` flag that submits a random 5% of the data to verify that the extraction logic and the costs are behaving as expected.
3.  **Human-in-the-Loop:** We generate sets of "Triage Samples" (200 pages per category) for manual visual inspection to ensure our "Mixed" and "Golden" logic is capturing the right data.

---

## Technical Metrics (Current Run)

| Metric | Value |
| :--- | :--- |
| **Total Corpus** | 422,973 Pages |
| **High-Value Target Pages** | ~83,000 Pages |
| **Extraction Model** | Gemini 1.5/2.5 Flash (Batch) |
| **Projected Cost (Full Run)** | **~$7.20 USD** |
| **Original Cost (Estimated w/o Triage)** | ~$40.00+ USD |

---

## Data Reconstruction (Harvesting)
Once the LLM finishes, it returns thousands of small JSON files. Our **Harvesting Script** performs the final assembly:
1.  **Metadata Join:** Uses the `chunk_map` to re-attach the original filename and page number to every extracted result.
2.  **Proximity Join:** Uses specialized logic to associate "Lab Results" with "Form 26R" metadata (waste codes, facility locations) found on preceding pages in the document.
3.  **Cleaning:** Standardizes units, removes duplicates, and flags "poor scans" for manual review.

**Status:** The pipeline is currently in the **Test Extraction** phase (5% sample run).
