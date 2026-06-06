# -*- coding: utf-8 -*-
"""
segment_lab_reports.py
----------------------
Implements a sequential page state-machine parser to scan the PDF page corpus
and group pages cover-to-cover by laboratory company.

Output:
    data/corpus/lab_report_corpus.parquet
"""

import os
import re
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'lab_report_corpus.parquet')

# ---------------------------------------------------------------------------
# Heuristic Patterns (from triage_lab_reports.py)
# ---------------------------------------------------------------------------
RE_UNITS = re.compile(r'\b(mg/[LI1l]|ug/[LI1l]|pCi/[LI1l]|mg/kg|ug/kg|Deg\.?\s*F|% Solids|wt%)\b', re.I)

RE_STRUCTURAL = re.compile(
    r'\b(Analyte|Parameter|Surrogate|Reporting\s+Limit|Detection\s+Limit|Method\s+Blank|'
    r'Matrix\s+Spike|LCS\s+Recovery|Batch\s+ID|QC\s+Result|Certificate\s+of\s+Analysis|'
    r'Analytical\s+Report|Case\s+Narrative|Chain\s+of\s+Custody|NELAP\s+Cert|'
    r'Sample\s+ID|Lab\s+Sample\s+ID|Client\s+Sample\s+ID|Project\s+Name|Job\s+ID|'
    r'Method\s+Reference|Work\s+Order|SDG|Data\s+Package|Collected:|Received:)\b', 
    re.I
)

RE_ANALYTES = re.compile(
    r'\b(Methanol|Chloride|Barium|Arsenic|Selenium|Radium-?226|Radium-?228|Benzene|'
    r'Toluene|Ethylbenzene|Xylenes?|Gross\s+Alpha|Gross\s+Beta|Flashpoint|Ignitability|'
    r'Specific\s+Conductance|Total\s+Dissolved\s+Solids|TDS|TSS|Oil\s+and\s+Grease|'
    r'Total\s+Suspended\s+Solids|Specific\s+Gravity|pH|Conductivity|Turbidity)\b', 
    re.I
)

RE_LABS = re.compile(
    r'\b(ALS\s+Environmental|TestAmerica|Eurofins|Pace\s+Analytical|Geochemical\s+Testing|'
    r'Microbac|Fairway\s+Lab|Mahaffey\s+Lab|REIC|Summit\s+Environmental|'
    r'American\s+Analytical|Lancaster\s+Labs|Paragon\s+Analytics|'
    r'Moody\s+and\s+Associates|Wetzel\s+Laboratories|Microbac\s+Laboratories)\b', 
    re.I
)

def calculate_triage_score(text):
    if not isinstance(text, str) or not text.strip():
        return 0, []

    matches = []
    score = 0

    unit_hits = RE_UNITS.findall(text)
    if unit_hits:
        score += len(set(unit_hits)) * 2
        matches.extend([f"UNIT:{m}" for m in set(unit_hits)])

    struct_hits = RE_STRUCTURAL.findall(text)
    if struct_hits:
        score += len(set(struct_hits)) * 3
        matches.extend([f"STRUCT:{m}" for m in set(struct_hits)])

    analyte_hits = RE_ANALYTES.findall(text)
    if analyte_hits:
        score += min(len(set(analyte_hits)), 5) * 2
        matches.extend([f"CHEM:{m}" for m in set(analyte_hits)])

    lab_hits = RE_LABS.findall(text)
    if lab_hits:
        score += 10
        matches.extend([f"LAB:{m}" for m in set(lab_hits)])

    return score, matches

# ---------------------------------------------------------------------------
# State-Machine Regex Triggers
# ---------------------------------------------------------------------------

RE_26R = re.compile(
    r'(?:2540-PM-BWM0347|FORM\s*26R|CHEMICAL\s*ANALYSIS\s*OF\s*RESIDUAL\s*WASTE)',
    re.I
)

LAB_REGEXES = {
    'ALS': re.compile(r'\b(ALS\s+Environmental|ALS\s+Group|ALS\s+Laboratory\s+Group|ALS\s+Canada)\b', re.I),
    'Eurofins': re.compile(r'\b(Eurofins|Lancaster\s+Laboratories|Eurofins\s+Lancaster)\b', re.I),
    'Geochemical': re.compile(r'\b(Geochemical\s+Testing|GEOCHEMICAL)\b', re.I),
    'Pace': re.compile(r'\b(Pace\s+Analytical\s+Services|Pace\s+Project\s+No|Pace\s+Analytical|[Aa]ce\s+Analytical)\b', re.I),
    'ESL': re.compile(r'\b(Environmental\s+Service\s+Laboratories|Env\s*.\s*Serv\s*.\s*Lab|ESL)\b', re.I),
    'Fairway': re.compile(r'\b(FAIRWAY\s+LABORATORIES|ANALYTICAL\s+REPORT\s+FOR\s+SAMPLES)\b', re.I),
    'Mahaffey': re.compile(r'\b(Mahaffey\s+Laboratory|Mehaffey|Curwensville,\s+PA\s+16833)\b', re.I),
    'Microbac': re.compile(r'\b(Microbac\s+Laboratories|Microbac|Ohio\s+Valley\s+Division|OVD)\b', re.I),
    'REIC': re.compile(r'\b(REI\s+Consultants,\s+Inc\.|REIC|improving\s+the\s+environment,\s+one\s+client\s+at\s+a\s+time|improving\s+the\s+environment,\s+one\s+colleague\s+at\s+a\s+time)\b', re.I),
    'Summit': re.compile(r'\b(SUMMIT\s+ENVIRONMENTAL\s+TECHNOLOGIES|Summit\s+Environmental|settek\.com)\b', re.I),
}

LAB_EXIT_REGEXES = {
    'Fairway': re.compile(r'\b(Chain\s+of\s+Custody\s+Receiving\s+Document|FLI0601-002)\b', re.I),
    'Mahaffey': re.compile(r'\b(Chain\s+of\s+Custody\s+Receiving\s+Document)\b', re.I),
    'Microbac': re.compile(r'\b(NELAP\s+Accreditation|List\s+of\s+Valid\s+Qualifiers|Qualkey|Accreditation\s+by\s+Laboratory\s+SOP)\b', re.I),
}

def segment_corpus():
    if not os.path.exists(CORPUS_PATH):
        print(f"Error: Corpus not found at {CORPUS_PATH}")
        return

    print(f"Loading corpus from {CORPUS_PATH}...")
    df = pd.read_parquet(CORPUS_PATH)
    print(f"Loaded {len(df)} pages.")

    print("Sorting corpus by file and page sequence...")
    df = df.sort_values(by=['set_name', 'filename', 'page_number']).reset_index(drop=True)

    print("Running state-machine segmentation...")
    current_file = None
    current_state = None
    next_page_reset = False

    detected_labs = []
    triage_scores = []
    matched_terms_list = []

    # Extract columns to native Python lists for extremely fast iteration
    set_names = df['set_name'].tolist()
    filenames = df['filename'].tolist()
    texts = df['text'].tolist()

    for i in tqdm(range(len(df)), desc="Segmenting"):
        set_name = set_names[i]
        filename = filenames[i]
        text = texts[i] if isinstance(texts[i], str) else ""

        file_key = (set_name, filename)
        if file_key != current_file:
            current_file = file_key
            current_state = None
            next_page_reset = False

        # Calculate triage score
        score, matched_terms = calculate_triage_score(text)
        triage_scores.append(score)
        matched_terms_list.append(", ".join(matched_terms))

        is_26r = bool(RE_26R.search(text))

        # Reset state from previous page exit trigger
        if next_page_reset:
            current_state = None
            next_page_reset = False

        # Match known labs
        matched_lab = None
        for lab_name, regex in LAB_REGEXES.items():
            if regex.search(text):
                matched_lab = lab_name
                break

        if is_26r:
            current_state = "26R"
        elif matched_lab:
            current_state = f"Lab_{matched_lab}"
        elif current_state in [None, "26R"]:
            # Check for high signal unrecognized lab reports
            if score >= 10 and not is_26r:
                current_state = "Lab_Other"

        # Check for exit signals
        if current_state and current_state.startswith("Lab_"):
            lab_name = current_state.split("_")[1]
            exit_regex = LAB_EXIT_REGEXES.get(lab_name)
            if exit_regex and exit_regex.search(text):
                next_page_reset = True
            
            # Reset "Other" if the score drops below 5
            if lab_name == "Other" and score < 5:
                current_state = None

        detected_labs.append(current_state)

    df['detected_lab'] = detected_labs
    df['triage_score'] = triage_scores
    df['matched_terms'] = matched_terms_list

    # Identify contiguous blocks of detected_lab
    df['is_lab'] = df['detected_lab'].str.startswith('Lab_', na=False)
    df['block_change'] = (
        (df['filename'] != df['filename'].shift()) |
        (df['detected_lab'] != df['detected_lab'].shift())
    )
    df['block_id'] = df['block_change'].cumsum()

    # Group by block_id to get start and end page numbers
    block_ranges = df[df['is_lab']].groupby('block_id')['page_number'].agg(['min', 'max'])

    # Map start and end back to the main DataFrame
    df['lab_start_page'] = df['block_id'].map(block_ranges['min']).astype('Int64')
    df['lab_end_page'] = df['block_id'].map(block_ranges['max']).astype('Int64')

    # Filter to only keep pages identified as being in a laboratory report
    lab_pages_df = df[df['detected_lab'].str.startswith('Lab_', na=False)].copy()
    
    # Drop temp helper columns
    lab_pages_df = lab_pages_df.drop(columns=['is_lab', 'block_change', 'block_id'])

    print(f"\nSegmentation complete.")
    print(f"Total lab report pages isolated: {len(lab_pages_df)} out of {len(df)} pages ({len(lab_pages_df)/len(df)*100:.1f}%)")
    print("\nPage count by detected lab category:")
    print(lab_pages_df['detected_lab'].value_counts())

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    lab_pages_df.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nSegmented corpus saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    segment_corpus()
