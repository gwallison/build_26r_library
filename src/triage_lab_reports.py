# -*- coding: utf-8 -*-
"""
triage_lab_reports.py
---------------------
Heuristic triage of the PDF corpus to identify pages likely to be Lab Reports.
Uses keyword and regex scoring based on units, chemical names, and lab headers.

Output:
    data/output/lab_report_triage.parquet
    data/output/lab_report_triage.html
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
HTML_OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.html')

# ---------------------------------------------------------------------------
# Heuristic Patterns
# ---------------------------------------------------------------------------

# Units (OCR resilient)
RE_UNITS = re.compile(r'\b(mg/[LI1l]|ug/[LI1l]|pCi/[LI1l]|mg/kg|ug/kg|Deg\.?\s*F|% Solids|wt%)\b', re.I)

# Structural Keywords (Expanded)
RE_STRUCTURAL = re.compile(
    r'\b(Analyte|Parameter|Surrogate|Reporting\s+Limit|Detection\s+Limit|Method\s+Blank|'
    r'Matrix\s+Spike|LCS\s+Recovery|Batch\s+ID|QC\s+Result|Certificate\s+of\s+Analysis|'
    r'Analytical\s+Report|Case\s+Narrative|Chain\s+of\s+Custody|NELAP\s+Cert|'
    r'Sample\s+ID|Lab\s+Sample\s+ID|Client\s+Sample\s+ID|Project\s+Name|Job\s+ID|'
    r'Method\s+Reference|Work\s+Order|SDG|Data\s+Package|Collected:|Received:)\b', 
    re.I
)

# Common Analytes (Signal for Lab Reports)
RE_ANALYTES = re.compile(
    r'\b(Methanol|Chloride|Barium|Arsenic|Selenium|Radium-?226|Radium-?228|Benzene|'
    r'Toluene|Ethylbenzene|Xylenes?|Gross\s+Alpha|Gross\s+Beta|Flashpoint|Ignitability|'
    r'Specific\s+Conductance|Total\s+Dissolved\s+Solids|TDS|TSS|Oil\s+and\s+Grease|'
    r'Total\s+Suspended\s+Solids|Specific\s+Gravity|pH|Conductivity|Turbidity)\b', 
    re.I
)

# Lab Names (Expanded)
RE_LABS = re.compile(
    r'\b(ALS\s+Environmental|TestAmerica|Eurofins|Pace\s+Analytical|Geochemical\s+Testing|'
    r'Microbac|Fairway\s+Lab|Mahaffey\s+Lab|REIC|Summit\s+Environmental|'
    r'American\s+Analytical|Lancaster\s+Labs|Paragon\s+Analytics|'
    r'Moody\s+and\s+Associates|Wetzel\s+Laboratories|Microbac\s+Laboratories)\b', 
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
        return

    print(f"Loading corpus from {CORPUS_PATH}...")
    df = pd.read_parquet(CORPUS_PATH)
    print(f"Loaded {len(df)} pages.")

    print("Running triage scoring...")
    # Apply scoring
    results = df['text'].apply(calculate_triage_score)
    df['triage_score'] = [r[0] for r in results]
    df['matched_terms'] = [", ".join(r[1]) for r in results]

    # Filter to pages that have at least some signal (score >= 5)
    triage_df = df[df['triage_score'] >= 5].copy()
    
    # Sort by score descending
    triage_df = triage_df.sort_values(by=['triage_score'], ascending=False)

    # Snippet for HTML/Inspection
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
