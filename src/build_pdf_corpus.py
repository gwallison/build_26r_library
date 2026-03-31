"""
build_pdf_corpus.py
-------------------
One-time build of a page-level text corpus from the full PDF library.
Output: data/corpus/pdf_corpus.parquet  (columns: set_name, filename, page_number, text)

Run once (or re-run whenever the PDF library gains new files):
    python build_pdf_corpus.py
"""

import os
import sys

import fitz  # PyMuPDF
import pandas as pd

# ---------------------------------------------------------------------------
# Constants  (keep in sync with text_search_all_pdf.py)
# ---------------------------------------------------------------------------

ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
DIRSET = ['2010-2018', '2019-2020', '2021-2022',
          '2023_all_months', '2024_all_months',
          'Jan_Jun_2025', 'Jul_Dec_2025']

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_DIR = os.path.join(PROJECT_ROOT, 'data', 'corpus')
CORPUS_PATH = os.path.join(CORPUS_DIR, 'pdf_corpus.parquet')


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_corpus(root: str, dirset: list, corpus_path: str) -> int:
    """
    Extract page text from every PDF and write to a parquet file.
    Returns the total number of pages written.
    """
    rows = []
    total_pdfs = 0

    for set_name in dirset:
        dir_path = os.path.join(root, set_name)
        if not os.path.isdir(dir_path):
            print(f"  Skipping missing directory: {dir_path}")
            continue

        pdf_files = sorted(
            f for f in os.listdir(dir_path) if f.lower().endswith('.pdf')
        )
        for fname in pdf_files:
            pdf_path = os.path.join(dir_path, fname)
            print(f"  Extracting: {set_name}/{fname}")
            try:
                doc = fitz.open(pdf_path)
                for page_index in range(len(doc)):
                    rows.append({
                        "set_name": set_name,
                        "filename": fname,
                        "page_number": page_index + 1,  # 1-based
                        "text": doc[page_index].get_text(),
                    })
                doc.close()
                total_pdfs += 1
            except Exception as e:
                print(f"  WARNING: could not read {pdf_path}: {e}")

    os.makedirs(os.path.dirname(corpus_path), exist_ok=True)
    df = pd.DataFrame(rows, columns=["set_name", "filename", "page_number", "text"])
    df.to_parquet(corpus_path, index=False)
    print(f"\nCorpus written to: {corpus_path}")
    print(f"  {total_pdfs} PDFs  |  {len(df)} pages")
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if os.path.exists(CORPUS_PATH) and "--rebuild" not in sys.argv:
        print(f"Corpus already exists: {CORPUS_PATH}")
        print("Pass --rebuild to overwrite it.")
        sys.exit(0)

    print(f"Building corpus from: {ROOT}")
    print(f"Output: {CORPUS_PATH}")
    print()
    build_corpus(ROOT, DIRSET, CORPUS_PATH)
