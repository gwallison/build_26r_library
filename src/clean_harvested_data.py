# -*- coding: utf-8 -*-
"""
clean_harvested_data.py
-----------------------
Automated rule-based cleanup for harvested laboratory data.
Fixes common LLM parsing errors and flags data quality issues.
"""

import os
import pandas as pd
import re
import csv
import io

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'output', 'batch_harvest_vertex')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'output', 'batch_cleaned_vertex')

RESULTS_FILE = os.path.join(INPUT_DIR, 'batch_harvest_results_vertex.parquet')
SAMPLES_FILE = os.path.join(INPUT_DIR, 'batch_harvest_samples_vertex.parquet')

CANONICAL_RESULTS = [
    "lab_sample_id", "analyte", "result", "reporting_limit", "mdl", 
    "units", "qualifier_code", "dilution_factor", "analysis_date", 
    "method", "pdf_page_number"
]

# ---------------------------------------------------------------------------
# Heuristics
# ---------------------------------------------------------------------------

def is_date(val):
    if not val or not isinstance(val, str): return False
    return bool(re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', val))

def is_unit(val):
    if not val or not isinstance(val, str): return False
    # Expanded unit list to catch common misalignments
    units = ['ug/l', 'mg/l', 'pci/g', 'pci/l', 's.u.', '°c', '%', 'mg/kg', 'ug/kg', 'deg f', 'mg caco3/l', 'ph units', 'umhos/cm', 'std. units', 'ml/100', 'g/mol']
    clean_val = val.lower().strip().replace('"', '').replace('.', '')
    return any(u in clean_val for u in units)

def is_numeric(val):
    if val is None or pd.isna(val) or val == "" or str(val).lower() in ['null', 'nan', 'none']: return False
    # Remove common numeric modifiers and trailing noise like quotes
    clean = re.sub(r'[<>\s\+/-]', '', str(val)).replace('"', '')
    try:
        float(clean)
        return True
    except:
        return str(val).upper() in ["ND"]

def get_data_quality_flags(row):
    """Checks for obvious data quality issues in numeric columns."""
    flags = []
    
    rl = str(row.get('reporting_limit', ''))
    if rl and rl.lower() not in ['null', 'nan', 'none', ''] and not is_numeric(rl):
        flags.append(f"Bad RL: {rl}")
        
    mdl = str(row.get('mdl', ''))
    if mdl and mdl.lower() not in ['null', 'nan', 'none', ''] and not is_numeric(mdl):
        flags.append(f"Bad MDL: {mdl}")
        
    return "; ".join(flags)

def align_tokens(tokens):
    """
    Core re-alignment logic using Unit and Date anchors.
    Canonical: [id, analyte, res, rl, mdl, UNIT, qual, dil, DATE, method, page]
    Indexes:   [ 0,    1,     2,   3,   4,   5,    6,    7,   8,      9,    10 ]
    """
    fixed = [None] * 11
    
    # 1. Date Anchor (Essential for the tail)
    date_idx = -1
    for i, t in enumerate(tokens):
        if is_date(t):
            date_idx = i
            break
    
    if date_idx == -1:
        return None # Can't anchor
        
    fixed[8] = tokens[date_idx]
    if len(tokens) > date_idx + 1: fixed[9] = tokens[date_idx+1]
    if len(tokens) > date_idx + 2: fixed[10] = tokens[date_idx+2]
    
    # 2. Unit Anchor
    unit_idx = -1
    for i, t in enumerate(tokens):
        if is_unit(t):
            unit_idx = i
            break
            
    # 3. ID and Analyte (Always first 2)
    fixed[0] = tokens[0]
    fixed[1] = tokens[1]
    
    if unit_idx != -1:
        fixed[5] = tokens[unit_idx]
        
        # Gap between Analyte and Unit: Result, RL, MDL
        gap1 = tokens[2:unit_idx]
        if len(gap1) >= 1: fixed[2] = gap1[0]
        if len(gap1) >= 2: fixed[3] = gap1[1]
        if len(gap1) >= 3: fixed[4] = gap1[2]
        
        # Gap between Unit and Date: Qual, Dil
        gap2 = tokens[unit_idx+1:date_idx]
        if len(gap2) >= 1:
            if is_numeric(gap2[0]): fixed[7] = gap2[0]
            else: fixed[6] = gap2[0]
        if len(gap2) >= 2:
            fixed[7] = gap2[1]
    else:
        # No unit found, Result is usually tokens[2]
        if len(tokens) > 2: fixed[2] = tokens[2]
        # Skip RL/MDL/Unit/Qual/Dil or try to guess?
        
    return fixed

def clean_results_row(row_dict):
    """
    Attempts to fix a single results row using type-based heuristics.
    """
    raw = row_dict.get('raw_csv_output', '')
    if not raw: return row_dict
    
    # Pre-cleaning the raw string
    raw_cleaned = raw.replace('","', '", "').replace(',,', ', "", ')
    
    try:
        reader = csv.reader([raw_cleaned], quotechar='"', skipinitialspace=True)
        tokens = next(reader)
    except:
        return row_dict
        
    new_row = row_dict.copy()
    new_row['cleaning_notes'] = ""
    
    # Attempt alignment
    fixed = align_tokens(tokens)
    
    if fixed:
        # Strip noise and nulls
        for i in range(11):
            if fixed[i]:
                fixed[i] = fixed[i].strip().strip('"')
                if fixed[i].lower() == 'null': fixed[i] = None
        
        for i, col in enumerate(CANONICAL_RESULTS):
            new_row[col] = fixed[i]
            
        new_row['is_flagged'] = False
        new_row['cleaning_notes'] = "Re-aligned via anchors."
    else:
        # Standard polish for non-aligned rows
        for col in CANONICAL_RESULTS:
            val = new_row.get(col)
            if val:
                val = str(val).strip().strip('"')
                if val.lower() == 'null': val = None
                new_row[col] = val

    # Final check for data quality
    new_row['data_quality_flags'] = get_data_quality_flags(new_row)
    
    return new_row

def clean_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if os.path.exists(RESULTS_FILE):
        print(f"Cleaning results: {RESULTS_FILE}")
        df = pd.read_parquet(RESULTS_FILE)
        
        records = df.to_dict('records')
        cleaned_records = [clean_results_row(r) for r in records]
        
        df_clean = pd.DataFrame(cleaned_records)
        df_clean.to_parquet(os.path.join(OUTPUT_DIR, 'batch_results_cleaned.parquet'), index=False)
        
        dq_flagged = len(df_clean[df_clean.data_quality_flags != ""])
        print(f"  Done. Rows with Data Quality Flags: {dq_flagged} / {len(df_clean)}")
        
    if os.path.exists(SAMPLES_FILE):
        print(f"Copying samples: {SAMPLES_FILE}")
        df_samples = pd.read_parquet(SAMPLES_FILE)
        df_samples.to_parquet(os.path.join(OUTPUT_DIR, 'batch_samples_cleaned.parquet'), index=False)

    print(f"\nCleanup complete. Files in: {OUTPUT_DIR}")

if __name__ == "__main__":
    clean_data()
