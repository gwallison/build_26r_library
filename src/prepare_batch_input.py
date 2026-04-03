# -*- coding: utf-8 -*-
"""
prepare_batch_input.py
----------------------
Generates a JSONL file for Gemini Batch API processing.
Selects 100 PDFs that have both Form 26R and Lab Report data.
"""

import os
import json
import pandas as pd
from get_single_file_prompt import get_file_prompt

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
GCS_ROOT = "gs://fta-form26r-library/full-set/"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_100.jsonl')

def prepare_batch():
    print("Loading metadata...")
    triage = pd.read_parquet(TRIAGE_PATH)
    f26r = pd.read_parquet(F26R_PATH)

    # Find files with both
    triage_files = triage[['set_name', 'filename']].drop_duplicates()
    f26r_files = f26r[['set_name', 'filename']].drop_duplicates()
    merged = pd.merge(triage_files, f26r_files, on=['set_name', 'filename'])

    print(f"Found {len(merged)} candidate files.")
    
    # Take first 100
    batch_files = merged.head(100)
    
    requests = []
    print(f"Generating prompts for 100 files...")
    
    for idx, row in batch_files.iterrows():
        set_name = row['set_name']
        filename = row['filename']
        
        # Get the specific prompt for this file
        prompt = get_file_prompt(set_name, filename)
        
        # Construct GCS URI
        # Note: GCS paths are case-sensitive and should match the structure
        gcs_uri = f"{GCS_ROOT}{set_name}/{filename}".replace(" ", "%20")
        # Wait, the user's example: 
        # https://storage.googleapis.com/fta-form26r-library/full-set/2010-2018/000044_BondiA_311_26R.pdf
        # Usually GCS URIs in gs:// format don't use %20, they use literal spaces if the tool supports it, 
        # but the Batch API might prefer literal or quoted. 
        # Actually, standard gsutil/gcloud supports spaces. 
        # The JSONL spec usually wants the URI as a string.
        gcs_uri = f"{GCS_ROOT}{set_name}/{filename}"
        
        # Create the request object with cachedContent
        # Format for Batch API with Cache:
        # {"request": {"contents": [...], "cachedContent": "cachedContents/..."}}
        request_obj = {
            "request": {
                "contents": [
                    {
                        "parts": [
                            {"text": prompt},
                            {
                                "fileData": {
                                    "mimeType": "application/pdf",
                                    "fileUri": gcs_uri
                                }
                            }
                        ]
                    }
                ],
                "cachedContent": "cachedContents/nvu80xthonydt25ycytyosmzhysfqlx9fexg52y8"
            }
        }
        requests.append(request_obj)
        
        if (idx + 1) % 10 != 0:
            pass # Keep it quiet
        else:
            print(f"  Processed {idx + 1}/100...")

    print(f"Writing {len(requests)} requests to {OUTPUT_JSONL}...")
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")

if __name__ == "__main__":
    prepare_batch()
