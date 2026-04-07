# -*- coding: utf-8 -*-
"""
probe_corpus_fuzzy.py (V2 - Optimized Regex)
---------------------
Probes the 420,000-page corpus using a single high-performance regex pattern 
built from the curated analyte list. 
"""

import os
import re
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
ANALYTE_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'core_analytes_review.csv')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'fuzzy_triage_results.parquet')

MIN_MATCHES_PER_PAGE = 3

def load_analytes():
    if not os.path.exists(ANALYTE_CSV):
        raise FileNotFoundError(f"Analyte list not found at {ANALYTE_CSV}")
    
    df = pd.read_csv(ANALYTE_CSV)
    analytes = df['clean_analyte'].dropna().unique().tolist()
    
    # Filter: 3+ chars, and escape special regex characters (e.g. Radium-226)
    analytes = [re.escape(a) for a in analytes if len(a) >= 3]
    return analytes

def run_probe():
    analytes = load_analytes()
    print(f"Loaded {len(analytes)} analytes for regex pattern.")
    
    # Build a single massive regex: (analyte1|analyte2|...)
    # We use word boundaries \b to ensure we don't match parts of words
    pattern_string = r'\b(' + '|'.join(analytes) + r')\b'
    RE_PROBE = re.compile(pattern_string, re.I)
    
    print(f"Loading corpus from {CORPUS_PATH}...")
    # Load in chunks if possible, but 1GB of text should fit in memory on most dev machines.
    df = pd.read_parquet(CORPUS_PATH, columns=['set_name', 'filename', 'page_number', 'text'])
    
    print(f"Probing {len(df)} pages...")
    
    match_counts = []
    match_lists = []
    
    # Process in bulk using the compiled regex
    for text in tqdm(df['text']):
        if not isinstance(text, str) or len(text) < 50:
            match_counts.append(0)
            match_lists.append("")
            continue
            
        # findall is fast; we take set() to count unique analytes found on the page
        found = RE_PROBE.findall(text)
        if found:
            unique_found = sorted(list(set([f.title() for f in found])))
            match_counts.append(len(unique_found))
            match_lists.append(", ".join(unique_found))
        else:
            match_counts.append(0)
            match_lists.append("")
    
    df['match_count'] = match_counts
    df['matched_analytes'] = match_lists
    
    # Filter to flagged pages
    flagged = df[df['match_count'] >= MIN_MATCHES_PER_PAGE].copy()
    flagged = flagged.drop(columns=['text'])
    
    print(f"\nProbe complete. Flagged {len(flagged)} pages.")
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    flagged.to_parquet(OUTPUT_PATH, index=False)
    print(f"Fuzzy triage results saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_probe()
