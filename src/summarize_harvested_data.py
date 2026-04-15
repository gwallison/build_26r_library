# -*- coding: utf-8 -*-
"""
summarize_harvested_data.py
---------------------------
Quick utility to analyze the current state of harvested V2 results.
"""

import pandas as pd
import os

RESULTS_PATH = "data/output/batch_harvest_surgical_v2/results_v2.parquet"

def summarize():
    if not os.path.exists(RESULTS_PATH):
        print(f"Error: {RESULTS_PATH} not found.")
        return

    df = pd.read_parquet(RESULTS_PATH)
    
    print("="*50)
    print("HARVESTED DATA SUMMARY (V2 SURGICAL)")
    print("="*50)
    print(f"Total Extraction Records: {len(df):,}")
    print(f"Unique Documents:         {df['original_filename'].nunique():,}")
    print(f"Total Lab Sample IDs:     {df['lab_sample_id'].nunique():,}")
    
    print("\nTOP 10 ANALYTES:")
    print("-" * 20)
    print(df['analyte'].value_counts().head(10))
    
    print("\nMATRIX DISTRIBUTION:")
    print("-" * 20)
    if 'matrix' in df.columns:
        print(df['matrix'].value_counts().head(5))
    else:
        print("Matrix metadata not yet fully merged.")

    print("\nDATA QUALITY FLAGS:")
    print("-" * 20)
    if 'is_poor_scan' in df.columns:
        bad_scans = df['is_poor_scan'].sum()
        print(f"Poor Scans Flagged: {bad_scans:,} ({bad_scans/len(df)*100:.1f}%)")
    
    # Check for empty results
    hits = len(df[df['analyte'] != ""])
    print(f"Total Rows with Data: {hits:,}")
    print("="*50)

if __name__ == "__main__":
    summarize()
