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
    - Standard `unicode61` index is best for most searches and supports `*` prefixing.
    - `trigram` (fuzzy) index is powerful for internal typos but requires at least one exact 3-char match.

---
## Work Log: April 27, 2026 (Part 2)
- **Task:** Improve search usability and handle high-volume output errors.
- **Updates:**
    - Added `set_name` column to both standard and fuzzy indexes for better context.
    - Added `--count-only` flag to `src/search_corpus.py` for exhaustive counting without data transfer.
    - Added `--csv` flag to `src/search_corpus.py` to save large result sets to disk, avoiding IOPub rate limits.
    - Improved console output formatting with truncated columns and clear headers.
