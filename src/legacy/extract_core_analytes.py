# -*- coding: utf-8 -*-
"""
extract_core_analytes.py
------------------------
Extracts unique analytes from harvested results, applies cleaning rules,
and exports a CSV for manual review before fuzzy probing.
"""

import os
import re
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'batch_harvest_surgical_v2', 'results_v2.parquet')
OUTPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'core_analytes_review.csv')

def clean_analyte(text):
    if not isinstance(text, str):
        return ""
        
    text = text.strip()
    
    # Remove parenthetical method notes: "pH (SM 4500 H+ B)" -> "pH"
    text = re.sub(r'\(.*?\)', '', text)
    
    # Remove "Total", "Dissolved", "Recoverable" qualifiers often appended
    # e.g., "Zinc, Total" -> "Zinc"
    text = re.sub(r',\s*(Total|Dissolved|Recoverable|Suspended)', '', text, flags=re.I)
    
    # Remove "as X"
    # e.g., "Nitrite as N" -> "Nitrite"
    text = re.sub(r'\s+as\s+[A-Z]\b', '', text, flags=re.I)
    
    # Clean up trailing/leading spaces or punctuation from the stripping
    text = re.sub(r'^[,\-\s]+|[,\-\s]+$', '', text)
    
    # Fix common OCR/typos in the names if obvious
    text = text.replace('Trichlorophenoi', 'Trichlorophenol')
    
    # Standardize casing to make deduplication more effective
    text = text.title()
    
    return text.strip()

def run_extraction():
    if not os.path.exists(RESULTS_PATH):
        print(f"Error: {RESULTS_PATH} not found.")
        return
        
    print(f"Loading results from {RESULTS_PATH}...")
    df = pd.read_parquet(RESULTS_PATH)
    
    raw_analytes = df['analyte'].dropna().unique()
    print(f"Found {len(raw_analytes)} raw unique analytes.")
    
    # Create DataFrame for review
    analyte_df = pd.DataFrame({'raw_analyte': raw_analytes})
    analyte_df['clean_analyte'] = analyte_df['raw_analyte'].apply(clean_analyte)
    
    # Filter out empty strings after cleaning
    analyte_df = analyte_df[analyte_df['clean_analyte'] != ""]
    
    # Get unique clean analytes, keeping the most frequent raw version as an example
    # First get frequency of raw analytes
    freq = df['analyte'].value_counts().reset_index()
    freq.columns = ['raw_analyte', 'frequency']
    
    analyte_df = pd.merge(analyte_df, freq, on='raw_analyte', how='left')
    
    # Sort by frequency so we can see the most common ones first
    analyte_df = analyte_df.sort_values(by='frequency', ascending=False)
    
    # Group by clean analyte to get distinct list
    unique_clean = analyte_df.groupby('clean_analyte').agg({
        'frequency': 'sum',
        'raw_analyte': lambda x: list(x)[:3] # Show up to 3 raw variants
    }).reset_index()
    
    unique_clean = unique_clean.sort_values(by='frequency', ascending=False)
    
    print(f"Consolidated into {len(unique_clean)} core analytes.")
    
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    unique_clean.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved review file to {OUTPUT_CSV}")

if __name__ == "__main__":
    run_extraction()
