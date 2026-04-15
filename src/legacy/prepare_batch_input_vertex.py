# -*- coding: utf-8 -*-
"""
prepare_batch_input_vertex.py
----------------------------
Generates a JSONL file for Vertex AI Gemini Batch processing.
Prepends 16 few-shot examples to every request to trigger implicit prefix caching.
"""

import os
import json
import pandas as pd
from get_single_file_prompt import SYSTEM_PROMPT, get_file_prompt

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
TRAINING_DIR = os.path.join(PROJECT_ROOT, 'data', 'training_examples')
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_vertex_100.jsonl')

GCS_TRAINING_ROOT = "gs://fta-form26r-library/training_sets"
GCS_BATCH_ROOT = "gs://fta-form26r-library/full-set"

def get_training_examples():
    """Builds the list of few-shot turns from local CSVs and GCS PDF references."""
    examples = []
    files = [f for f in os.listdir(TRAINING_DIR) if f.startswith("training_") and f.endswith(".pdf")]
    
    for filename in sorted(files):
        pdf_gcs_uri = f"{GCS_TRAINING_ROOT}/{filename}"
        csv_filename = filename.replace("training_", "output_").replace(".pdf", ".csv")
        csv_path = os.path.join(TRAINING_DIR, csv_filename)

        if not os.path.exists(csv_path):
            continue

        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_text = f.read()

        # User turn (the PDF)
        examples.append({
            "role": "user",
            "parts": [
                {"text": f"Extract results from this file: {filename}"},
                {"fileData": {"mimeType": "application/pdf", "fileUri": pdf_gcs_uri}}
            ]
        })
        # Model turn (the CSV)
        examples.append({
            "role": "model",
            "parts": [{"text": csv_text}]
        })
    return examples

def prepare_batch():
    print("Loading metadata...")
    triage = pd.read_parquet(TRIAGE_PATH)
    f26r = pd.read_parquet(F26R_PATH)

    # Find files with both
    triage_files = triage[['set_name', 'filename']].drop_duplicates()
    f26r_files = f26r[['set_name', 'filename']].drop_duplicates()
    merged = pd.merge(triage_files, f26r_files, on=['set_name', 'filename'])

    print(f"Found {len(merged)} candidate files.")
    batch_files = merged.head(100)
    
    print("Building few-shot examples...")
    training_turns = get_training_examples()
    print(f"  Loaded {len(training_turns)//2} examples.")

    requests = []
    print(f"Preparing 100 requests...")
    
    for idx, row in batch_files.iterrows():
        set_name = row['set_name']
        filename = row['filename']
        
        # Get the specific prompt for this file (includes System Prompt + Guide)
        prompt = get_file_prompt(set_name, filename)
        
        # Vertex GCS URI for the target file
        file_gcs_uri = f"{GCS_BATCH_ROOT}/{set_name}/{filename}".replace("\\", "/")
        
        # Build the contents list:
        # 1. Few-shot turns (training_turns)
        # 2. Final user turn for the target file
        contents = list(training_turns)
        contents.append({
            "role": "user",
            "parts": [
                {"text": prompt},
                {"fileData": {"mimeType": "application/pdf", "fileUri": file_gcs_uri}}
            ]
        })

        # Vertex AI Batch Request Format
        # Using full GCS URI as ID for perfect traceability
        request_obj = {
            "id": file_gcs_uri,
            "request": {
                "contents": contents
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
