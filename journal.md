# Project Journal: Search Indexing & OCR Handling

## Search Capabilities (SQLite FTS5)

The search index is **case-insensitive** by default (using the `unicode61` tokenizer). Searching for `benzene`, `Benzene`, or `BENZENE` will return the same hits.

### Complex Search Examples

| Query Type | Syntax Example | Description |
| :--- | :--- | :--- |
| **Proximity** | `NEAR(Benzene Mountain, 10)` | Words within 10 tokens of each other. |
| **Boolean** | `Benzene AND Toluene NOT QA` | Must have both, exclude quality assurance. |
| **Exact Phrase** | `"Ground Water"` | Match the literal phrase. |
| **Fuzzy (OCR)** | `python src/search_corpus.py "Benzen3" --fuzzy` | Uses trigram index to find typos/OCR errors. |
| **Prefix** | `Methyl*` | Matches Methyl, Methylene, Methylbenzene, etc. |
| **Grouping** | `(Benzene OR Toluene) AND Altoona` | Logical grouping with parentheses. |

### Technical Details
- **Standard Index:** `pages_idx` (fast, word-based).
- **Fuzzy Index:** `pages_fuzzy` (trigram-based, handles 1-2 character errors).
- **Database Path:** `data/corpus/corpus_search.db` (~6GB).

### Tips for Command Line
- Wrap the whole query in double quotes: `python src/search_corpus.py "Benzene AND Toluene"`
- If searching for an exact phrase inside a query, use nested quotes or escaped quotes depending on your shell.

---
## Work Log: April 27, 2026
- **Task:** Implement fast search index for the PDF corpus.
- **Solution:** Created a SQLite FTS5 database (`corpus_search.db`) containing:
    - `pages_idx`: Standard word-based index using `unicode61` (case-insensitive).
    - `pages_fuzzy`: Trigram-based index for handling OCR errors/typos.
- **Tools Created:**
    - `src/build_search_index.py`: Builds standard index.
    - `src/build_fuzzy_index.py`: Builds trigram index.
    - `src/search_corpus.py`: Interactive CLI tool for complex queries (AND, NEAR, phrase, fuzzy).
    - `src/batch_search.py`: Tool for searching multiple terms and saving results to Parquet.
- **Lessons:** 
    - standard `unicode61` index is best for most searches and supports `*` prefixing.
    - `trigram` (fuzzy) index is powerful for internal typos but requires at least one exact 3-char match.

---
## Work Log: April 30, 2026
- **Task:** Create a tool to find all fuzzy variants of a term or phrase in the corpus.
- **Solution:** Developed `src/get_variants.py` which leverages the FTS5 dictionary and fuzzy index.
- **Capabilities:**
    - **Single Words:** Uses `fts5vocab` and `rapidfuzz` to find dictionary variants in ~0.05s.
    - **Joined Phrases:** Detects phrases indexed as single words (e.g., "rangeresources").
    - **Proximity Phrases:** Scans the fuzzy index for multi-word variants (e.g., "Range Resour").
    - **CSV Output:** Exports structured data including match type, scores, and hit counts.
- **Lessons:**
    - Querying the `fts5vocab` table is much faster for finding variants than scanning the entire corpus text.
    - Combining word-level fuzzy matching with index-level proximity searches allows for robust phrase variant detection despite OCR errors.

---
## Work Log: May 6, 2026
- **Task:** Exploratory analysis of corpus search results.
- **Solution:** Created `src/spike_search_corpus.ipynb` to prototype advanced search interactions and visualization.
- **Lessons:** 
    - Visualizing snippets in a notebook is much more efficient for rapid triage than CLI output.
    - Need a more permanent GUI for non-technical stakeholders to explore the 420,000 pages.

---
## Work Log: May 7, 2026
- **Task:** Refactor `src/search_corpus.py` for integration and usability.
- **Updates:**
    - Added `verbose` flag to suppress output when called programmatically.
    - Modified function to return hit counts, enabling integration into larger pipeline scripts.
    - Standardized internal snippet highlighting across tools.

---
## Work Log: May 13, 2026
- **Task:** Build a permanent visual search interface and finalize V2 Pipeline documentation.
- **Solution:** 
    - Developed `src/app_search.py` using **Streamlit**.
    - **Features:** 
        - Integrated "Variant Finder" in the sidebar for real-time OCR typo detection.
        - Interactive results table with single-row selection.
        - "Inspector" panel that shows the full page text with highlighted search terms.
        - CSV download of search results for downstream analysis.
    - Confirmed **Surgical V2 Pipeline** state: "Production-Ready" after successful 5% sample and final full-run.
    - Updated `GEMINI.md` to reflect current project status and GCP environment requirements (`open-ff-catalog-1`).
    - **Lessons:**
        - Streamlit is extremely effective for building "human-in-the-loop" verification tools for large-scale data extraction.
        - The "Inspector" pattern (Search -> Select -> Full Text) is critical for validating whether a page hit is actually relevant before committing to LLM extraction costs.

---
## Work Log: April 27, 2026 (Part 2)
- **Task:** Improve search usability and handle high-volume output errors.
- **Updates:**
    - Added `set_name` column to both standard and fuzzy indexes for better context.
    - Added `--count-only` flag to `src/search_corpus.py` for exhaustive counting without data transfer.
    - Added `--csv` flag to `src/search_corpus.py` to save large result sets to disk, avoiding IOPub rate limits.
    - Improved console output formatting with truncated columns and clear headers.

---
## Work Log: June 5, 2026
- **Task 1:** Eliminate the performance bottleneck on row selection click in `app_lab_search.py` caused by the runtime database query in `get_consecutive_lab_pages()`.
  - **Solution:** 
    - Updated `src/segment_lab_reports.py` to pre-calculate contiguous lab report page ranges (`lab_start_page` and `lab_end_page`) using cumulative sum grouping on consecutive `detected_lab` sequences.
    - Updated `src/build_lab_index.py` to include `lab_start_page UNINDEXED` and `lab_end_page UNINDEXED` columns in the SQLite FTS5 `pages_idx` and `pages_fuzzy` schemas, populating them during bulk index creation.
    - Modified `src/app_lab_search.py` to select the pre-calculated `lab_start_page` and `lab_end_page` columns directly in FTS search queries.
    - Deleted the slow runtime SQL-based `get_consecutive_lab_pages()` function and replaced it with direct, sub-millisecond in-memory list range generation.
  - **Results:**
    - Database regenerated and rebuilt successfully in the 4.65 GB `lab_report_corpus.db`.
    - Consecutive page calculation is now instantaneous upon selection in Streamlit, completely removing the multi-second G-drive network/sync lock query bottleneck.
- **Task 2:** Add a feature to download a zipped archive of the clipped lab report PDFs matching the current search table.
  - **Solution:**
    - Added a `📦 Build & Zip Clipped Lab Report PDFs` button in the Streamlit app.
    - It groups search results to identify unique lab report segments, locates the original PDFs at `D:\PA_Form26r_PDFs\all_pdfs`, clips out the full consecutive page ranges from `lab_start_page` to `lab_end_page` using PyMuPDF (`fitz`), and packages them into a clean `.zip` archive named `[original_name]_pages_[start]_[end].pdf` for immediate download.
- **Task 3:** Fix LLM extraction repetition loops and validate Scenario C pipeline.
  - **Identified Issue:** Under `gemini-2.5-flash`, the model got stuck in infinite repetition loops outputting hundreds of timezone permutations in the `rd`/`cd` date fields, causing JSON truncation and 100% parsing failure.
  - **Solution:** 
    - Updated `src/prepare_batch_input_surgical_v2.py` and response schemas to use Gemini's native `"format": "date"` and `"nullable": true` constraints.
    - Updated system instructions and added automatic date parsing/formatting (`clean_date_to_iso`) in prompt-building step so few-shot training examples strictly match `YYYY-MM-DD`.
    - Grammar-constrained decoding at the API level successfully prevented any invalid tokens or repetition loops.
  - **Results:**
    - Re-run batch prediction job completed in <3 minutes with **100% parsing success** across all chunks.
    - Extracted 44 high-fidelity result rows with clean `YYYY-MM-DD` ISO dates.
    - Total cost for the validation run was **$0.1072** (10 cents).
    - Removed forward-fill and backward-fill metadata propagation based on user feedback to prevent false associations across unrelated report pages.
    - Verified results rendered correctly side-by-side with original page context in the Streamlit app.
- **Task 4:** Scale up to 10% Scenario C extraction.
  - **Solution:** 
    - Wrote `src/prepare_10pct_scenario_c.py` to sample 10% (702 files, 863 chunks, 2,589 pages) from already physically split PDFs on GCS.
    - Generated a dedicated input JSONL `data/batch_input_scenario_c_10pct.jsonl`.
    - Wrote `src/run_batch_job_scenario_c_10pct.py` to submit the job using `gemini-2.5-flash` with thinking budget disabled.
    - Wrote `src/harvest_batch_results_scenario_c_10pct.py` to harvest results, map back to original files/pages, filter out surrogate/QC rows, and keep page-specific metadata clean without propagation.
    - Updated `src/app_lab_search.py` to dynamically load the 10% parquet when available.
  - **Results:**
    - Job completed successfully on Vertex AI in ~8 minutes.
    - Achieved a **99.88% parsing success rate** (862 out of 863 chunks parsed perfectly).
    - Extracted **56,165 genuine analyte records** from 2,589 pages.
    - Automatically filtered out 892 surrogate/QC rows.
    - Total run cost was only **$1.6160** (approx. $1.62).
    - Results loaded cleanly into the search app.
- **Task 5 (Post-10% Run):** Resolve false metadata propagation/filling across pages.
  - **Identified Issue:** Joining sample-level metadata (such as Project Name, Client Name, Lab Name, Lab Report ID) back onto results resulted in false associations on pages where they were not written.
  - **Solution:** 
    - Implemented a fast, vectorized page-text validation step in `harvest_batch_results_scenario_c_10pct.py` and `harvest_batch_results_surgical_v2.py`.
    - It reads the 272 MB `lab_report_corpus.parquet` file in memory, extracts the raw OCR text for all unique result pages, and checks for the presence of the extracted metadata values.
    - If a metadata term (e.g. Project Name) is not present (either exactly or via fallback alphanumeric cleaning) in the raw page text, it is set to `None`.
  - **Results:**
    - Completed validation check on 56,165 results rows in just **2.22 seconds**.
    - Cleared 1,435 false project names (15.53%), 551 client names (7.34%), 1,640 lab names (21.39%), and 1,352 lab report IDs (17.88%) that had bled onto pages where they did not belong.
    - Successfully updated `results_scenario_c_10pct.parquet` which renders immediately in the Streamlit app.

