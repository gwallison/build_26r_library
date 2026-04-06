# -*- coding: utf-8 -*-
"""
manager_select_next_batch.py
----------------------------
Orchestrates the selection of the next set of files for processing.
1. Filters out already processed files (from processed_files.parquet).
2. Filters out files > 50MB (local check).
3. Selects the next N files from the triaged corpus.
4. Triggers the physical splitting script.
"""

import os
import pandas as pd
from split_pdfs import split_target_files

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
PROCESSED_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'processed_files.parquet')
PDF_LIBRARY_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"

BATCH_SIZE = 500
MAX_MB = 50

def select_and_split():
    print("--- BATCH MANAGER: STARTING SELECTION ---")
    
    # 1. Load Metadata
    triage = pd.read_parquet(TRIAGE_PATH)
    f26r = pd.read_parquet(F26R_PATH)
    
    if os.path.exists(PROCESSED_PATH):
        processed = pd.read_parquet(PROCESSED_PATH)
    else:
        processed = pd.DataFrame(columns=['set_name', 'filename'])

    # 2. Get All potential files (Intersection of triage and f26r)
    triage_files = triage[['set_name', 'filename']].drop_duplicates()
    f26r_files = f26r[['set_name', 'filename']].drop_duplicates()
    merged = pd.merge(triage_files, f26r_files, on=['set_name', 'filename'])
    
    print(f"Total files with lab triage signal: {len(merged)}")

    # 3. Filter out processed
    # Create a unique key for comparison
    merged['key'] = merged['set_name'] + "|||" + merged['filename']
    processed['key'] = processed['set_name'] + "|||" + processed['filename']
    
    available = merged[~merged['key'].isin(processed['key'])].copy()
    print(f"Files not yet processed: {len(available)}")

    # 4. Filter by file size (> 50MB)
    def get_size_mb(row):
        pdf_path = os.path.join(PDF_LIBRARY_ROOT, row['set_name'], row['filename'])
        if not os.path.exists(pdf_path):
            pdf_path = os.path.join(PDF_LIBRARY_ROOT, row['filename'])
        if os.path.exists(pdf_path):
            return os.path.getsize(pdf_path) / (1024 * 1024)
        return 9999 # Treat missing as too big/bad

    print(f"Checking file sizes (limit {MAX_MB}MB)...")
    available['mb'] = available.apply(get_size_mb, axis=1)
    
    valid = available[available['mb'] <= MAX_MB].copy()
    too_big = len(available) - len(valid)
    print(f"Files excluded (> 50MB or missing): {too_big}")
    print(f"Files valid for next batch: {len(valid)}")

    # 5. Select next BATCH_SIZE
    next_batch = valid.head(BATCH_SIZE)
    print(f"\n--- SELECTED NEXT {len(next_batch)} FILES ---")

    # 6. Trigger Split
    print("Triggering physical splitting...")
    num_chunks = split_target_files(next_batch)
    
    print(f"\n--- SUCCESS ---")
    print(f"Prepared {len(next_batch)} files into {num_chunks} Micro-PDFs.")
    print(f"Ready for Batch Prep V2.")

if __name__ == "__main__":
    select_and_split()
