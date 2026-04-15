import os
import re
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
ANALYTE_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'core_analytes_trimmed.csv')
LC_MARKER_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_control_markers.csv')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'fuzzy_triage_results.parquet')

# Minimum unique analyte matches to consider a page "Target"
MIN_MATCHES_PER_PAGE = 3

# High-certainty Analytical Indicators (Positive markers for sample reports)
POSITIVE_MARKERS = [
    r"Client Sample ID",
    r"Lab Sample ID",
    r"Date Collected",
    r"Date Received",
    r"Matrix: Water",
    r"Matrix: Soil",
    r"Analytical Results"
]

def load_patterns():
    # 1. Load Analytes (Trimmed list)
    if not os.path.exists(ANALYTE_CSV):
        raise FileNotFoundError(f"Analyte list not found at {ANALYTE_CSV}")
    
    df_a = pd.read_csv(ANALYTE_CSV)
    analytes = sorted(df_a['clean_analyte'].dropna().unique().tolist(), key=len, reverse=True)
    
    # Use non-capturing group for speed (?:...)
    analyte_pattern = r'\b(?:' + '|'.join([re.escape(a) for a in analytes if len(a) >= 3]) + r')\b'
    re_analyte = re.compile(analyte_pattern, re.I)
    
    # 2. Load Lab Control Markers
    if not os.path.exists(LC_MARKER_CSV):
        raise FileNotFoundError(f"LC markers not found at {LC_MARKER_CSV}")
    
    df_lc = pd.read_csv(LC_MARKER_CSV)
    markers = df_lc['term'].dropna().unique().tolist()
    lc_pattern = r'\b(?:' + '|'.join([re.escape(m) for m in markers]) + r')\b'
    re_lc = re.compile(lc_pattern, re.I)

    # 3. Positive Markers
    pos_pattern = r'\b(?:' + '|'.join(POSITIVE_MARKERS) + r')\b'
    re_pos = re.compile(pos_pattern, re.I)
    
    return re_analyte, re_lc, re_pos

def run_probe():
    re_analyte, re_lc, re_pos = load_patterns()
    print(f"Loaded patterns for analytes, LC markers, and positive indicators.")
    
    print(f"Loading corpus from {CORPUS_PATH}...")
    # Load columns needed for triage
    df = pd.read_parquet(CORPUS_PATH, columns=['set_name', 'filename', 'page_number', 'text'])
    
    print(f"Probing {len(df)} pages...")
    
    match_counts = []
    is_lc_list = []
    has_pos_list = []
    
    for text in tqdm(df['text'], desc="Triage Progress"):
        if not isinstance(text, str) or len(text) < 50:
            match_counts.append(0)
            is_lc_list.append(False)
            has_pos_list.append(False)
            continue
            
        # A. Check for Lab Control Markers (Stop at first match for speed)
        is_lc = bool(re_lc.search(text))
        is_lc_list.append(is_lc)
        
        # B. Check for Positive Analytical Markers
        has_pos = bool(re_pos.search(text))
        has_pos_list.append(has_pos)
        
        # C. Count Unique Analytes (Limit search for speed if possible, but findall is okay for 100)
        found = re_analyte.findall(text)
        if found:
            unique_found = len(set([f.lower() for f in found]))
            match_counts.append(unique_found)
        else:
            match_counts.append(0)
    
    df['match_count'] = match_counts
    df['is_lab_control'] = is_lc_list
    df['has_pos_marker'] = has_pos_list
    
    # Categorize for Surgical Selection:
    def categorize(row):
        if row['match_count'] >= MIN_MATCHES_PER_PAGE:
            if row['has_pos_marker']:
                if not row['is_lab_control']:
                    return "Golden"      # High Priority: Clean data
                else:
                    return "Mixed"       # Medium Priority: Data + QC on same page
            else:
                if not row['is_lab_control']:
                    return "Continuation" # Low/Med Priority: Likely middle of a table
                else:
                    return "Pure_QC"      # Exclude: Hits LC but no sample IDs
        elif row['is_lab_control'] and not row['has_pos_marker']:
            return "Pure_QC"              # Exclude: No analytes, just LC markers
        return "Noise"                    # Exclude: No analytes, no markers

    print("Categorizing pages...")
    df['triage_category'] = df.apply(categorize, axis=1)
    
    # Filter to anything that isn't Noise or Pure_QC (unless we want to keep Pure_QC for some reason)
    # Actually, let's keep everything that isn't "Noise" in the triage output for visibility
    flagged = df[df['triage_category'] != "Noise"].copy()
    
    # Clean up memory by dropping text before saving results
    if 'text' in flagged.columns:
        flagged = flagged.drop(columns=['text'])
    
    print(f"\nProbe complete.")
    print("Triage Category Breakdown:")
    print(flagged['triage_category'].value_counts())
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    flagged.to_parquet(OUTPUT_PATH, index=False)
    print(f"Fuzzy triage results saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    run_probe()
