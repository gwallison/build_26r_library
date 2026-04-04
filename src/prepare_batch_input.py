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
PDF_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_100.jsonl')
API_KEY = os.getenv("GoogleAI-API-key")

from google import genai
client = genai.Client(api_key=API_KEY)

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
    
    # Pre-check existing files in File API to avoid re-uploading
    print("Checking existing files in File API...")
    existing_files = {f.display_name: f.name for f in client.files.list()}
    
    requests = []
    print(f"Uploading/Mapping 100 files...")
    
    for idx, row in batch_files.iterrows():
        set_name = row['set_name']
        filename = row['filename']
        
        # Get the specific prompt for this file
        prompt = get_file_prompt(set_name, filename)
        
        # Check if already uploaded
        display_name = f"batch_{set_name}_{filename}".replace(" ", "_")
        if display_name in existing_files:
            file_name = existing_files[display_name]
        else:
            file_path = os.path.join(PDF_ROOT, set_name, filename)
            if not os.path.exists(file_path):
                print(f"  Warning: File not found: {file_path}")
                continue
            
            # Upload
            uploaded_file = client.files.upload(
                file=file_path,
                config={'display_name': display_name}
            )
            file_name = uploaded_file.name
            existing_files[display_name] = file_name
            
        # Create the request object (Google AI Batch API schema)
        request_obj = {
            "id": filename.replace(".pdf", ""),
            "request": {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": prompt},
                            {
                                "fileData": {
                                    "mimeType": "application/pdf",
                                    "fileUri": f"https://generativelanguage.googleapis.com/v1beta/{file_name}"
                                }
                            }
                        ]
                    }
                ],
                "cachedContent": "cachedContents/nvu80xthonydt25ycytyosmzhysfqlx9fexg52y8"
            }
        }
        requests.append(request_obj)
        
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1}/100...")

    print(f"Writing {len(requests)} requests to {OUTPUT_JSONL}...")
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")

if __name__ == "__main__":
    prepare_batch()
