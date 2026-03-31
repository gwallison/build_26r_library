# -*- coding: utf-8 -*-
"""
triage_lab_reports.py
---------------------
Heuristic triage of the PDF corpus to identify pages likely to be Lab Reports.
Uses keyword and regex scoring based on units, chemical names, and lab headers.

Output:
    data/output/lab_report_triage.parquet
"""

import os
import re
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')

# ---------------------------------------------------------------------------
# Heuristic Patterns
# ---------------------------------------------------------------------------

# Units (OCR resilient)
RE_UNITS = re.compile(r'\b(mg/[LI1l]|ug/[LI1l]|pCi/[LI1l]|mg/kg|ug/kg|Deg\.?\s*F)\b', re.I)

# Structural Keywords
RE_STRUCTURAL = re.compile(
    r'\b(Analyte|Parameter|Surrogate|Reporting\s+Limit|Detection\s+Limit|Method\s+Blank|'
    r'Matrix\s+Spike|LCS\s+Recovery|Batch\s+ID|QC\s+Result|Certificate\s+of\s+Analysis|'
    r'Analytical\s+Report|Case\s+Narrative|Chain\s+of\s+Custody|NELAP\s+Cert)\b', 
    re.I
)

# Common Analytes (Signal for Lab Reports)
RE_ANALYTES = re.compile(
    r'\b(Methanol|Chloride|Barium|Arsenic|Selenium|Radium-?226|Radium-?228|Benzene|'
    r'Toluene|Ethylbenzene|Xylenes?|Gross\s+Alpha|Gross\s+Beta|Flashpoint|Ignitability|'
    r'Specific\s+Conductance|Total\s+Dissolved\s+Solids|TDS|TSS)\b', 
    re.I
)

# Lab Names
RE_LABS = re.compile(
    r'\b(ALS\s+Environmental|TestAmerica|Eurofins|Pace\s+Analytical|Geochemical\s+Testing|'
    r'Microbac|Fairway\s+Lab|Mahaffey\s+Lab|REIC|Summit\s+Environmental)\b', 
    re.I
)

def calculate_triage_score(text):
    """
    Calculate a heuristic score for a page based on pattern matches.
    Returns (score, matched_terms_list)
    """
    if not isinstance(text, str) or not text.strip():
        return 0, []

    matches = []
    score = 0

    # Units are strong indicators
    unit_hits = RE_UNITS.findall(text)
    if unit_hits:
        score += len(set(unit_hits)) * 2
        matches.extend([f"UNIT:{m}" for m in set(unit_hits)])

    # Structural keywords
    struct_hits = RE_STRUCTURAL.findall(text)
    if struct_hits:
        score += len(set(struct_hits)) * 3
        matches.extend([f"STRUCT:{m}" for m in set(struct_hits)])

    # Analytes
    analyte_hits = RE_ANALYTES.findall(text)
    if analyte_hits:
        score += min(len(set(analyte_hits)), 5) * 2 # Cap analyte contribution
        matches.extend([f"CHEM:{m}" for m in set(analyte_hits)])

    # Lab names (very strong indicator)
    lab_hits = RE_LABS.findall(text)
    if lab_hits:
        score += 10
        matches.extend([f"LAB:{m}" for m in set(lab_hits)])

    return score, matches

def run_triage():
    if not os.path.exists(CORPUS_PATH):
        print(f"Error: Corpus not found at {CORPUS_PATH}")
        print("Please run 'python src/build_pdf_corpus.py' first.")
        return

    print(f"Loading corpus from {CORPUS_PATH}...")
    df = pd.read_parquet(CORPUS_PATH)
    print(f"Loaded {len(df)} pages.")

    print("Running triage scoring...")
    # Apply scoring
    results = df['text'].apply(calculate_triage_score)
    df['triage_score'] = [r[0] for r in results]
    df['matched_terms'] = [", ".join(r[1]) for r in results]

    # Filter to pages that have at least some signal
    # A score of 5 is a reasonable threshold for "likely a lab report" 
    # (e.g., one structural keyword + one unit, or one analyte + one unit)
    triage_df = df[df['triage_score'] >= 5].copy()
    
    # Sort by score descending
    triage_df = triage_df.sort_values(by=['triage_score'], ascending=False)

    # Drop the full text to keep the output file small, but keep a snippet if needed
    # Actually, the user might want to see the text in the triage results, 
    # but for a parquet index, we usually don't need the full text.
    # Let's keep a snippet (first 200 chars) for quick inspection.
    triage_df['text_snippet'] = triage_df['text'].str.slice(0, 200).str.replace('\n', ' ')
    
    # Select columns for final output
    output_cols = [
        'set_name', 'filename', 'page_number', 'triage_score', 
        'matched_terms', 'text_snippet'
    ]
    
    print(f"Triage complete. Flagged {len(triage_df)} pages (score >= 5).")
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    triage_df[output_cols].to_parquet(OUTPUT_PATH, index=False)
    print(f"Results written to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_triage()
