# -*- coding: utf-8 -*-
"""
harvest_batch_results_company.py
--------------------------------
Downloads and parses the company name extraction batch results.
Maps back to original filenames and saves to a final parquet.
"""

import os
import json
import pandas as pd
from google.cloud import storage
from google import genai

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
JOB_ID_FILE = "data/current_batch_job_company.txt"
OUTPUT_DIR = "data/output/batch_harvest_company"

# Key mapping: Short -> Descriptive
KEY_MAP = {
    "c": "client_name",
    "l": "lab_name",
    "conf": "confidence"
}

def harvest_results():
    if not os.path.exists(JOB_ID_FILE):
        print(f"Error: {JOB_ID_FILE} not found.")
        return

    with open(JOB_ID_FILE, "r") as f:
        job_id = f.read().strip()

    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    job = client.batches.get(name=job_id)

    if job.state.name != "JOB_STATE_SUCCEEDED":
        print(f"Job is not finished. Current state: {job.state.name}")
        return

    dest_uri = job.dest.gcs_uri
    print(f"Parent output location: {dest_uri}")

    storage_client = storage.Client(project=PROJECT_ID)
    bucket_name = dest_uri.replace("gs://", "").split("/")[0]
    prefix = "/".join(dest_uri.replace("gs://", "").split("/")[1:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    extracted_records = []
    
    total_prompt_tokens = 0
    total_candidate_tokens = 0

    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    jsonl_blobs = [b for b in blobs if b.name.endswith("predictions.jsonl")]
    print(f"Found {len(jsonl_blobs)} prediction files.")

    for blob in jsonl_blobs:
        subfolder = os.path.basename(os.path.dirname(blob.name)).replace(":", "_")
        local_job_dir = os.path.join(OUTPUT_DIR, subfolder)
        os.makedirs(local_job_dir, exist_ok=True)
        local_file = os.path.join(local_job_dir, os.path.basename(blob.name))
        
        if not os.path.exists(local_file):
            blob.download_to_filename(local_file)

        with open(local_file, "r", encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    usage = data.get('response', {}).get('usageMetadata', {})
                    total_prompt_tokens += usage.get('promptTokenCount', 0)
                    total_candidate_tokens += usage.get('candidatesTokenCount', 0)

                    # ID is the original filename
                    filename = data.get('id', '')
                    if '|||' in filename:
                        filename = filename.split('|||')[-1]
                    
                    if 'response' in data and 'candidates' in data['response']:
                        content = data['response']['candidates'][0]['content']['parts'][0]['text']
                        payload = json.loads(content)
                        
                        record = {KEY_MAP.get(k, k): v for k, v in payload.items()}
                        record['filename'] = filename
                        extracted_records.append(record)
                except Exception:
                    pass

    if not extracted_records:
        print("No results found to harvest.")
        return

    print(f"Harvested {len(extracted_records)} records.")

    # Create DataFrame
    df = pd.DataFrame(extracted_records)
    
    # Save Results
    final_output_path = "data/output/company_extraction_results.parquet"
    df.to_parquet(final_output_path, index=False)
    print(f"Saved results to {final_output_path}")

    # Cost Summary
    PRICE_INPUT = 0.075 / 1_000_000
    PRICE_OUTPUT = 0.30 / 1_000_000
    likely_cost = (total_prompt_tokens * PRICE_INPUT) + (total_candidate_tokens * PRICE_OUTPUT)

    print("\n" + "="*50)
    print("BATCH COST INVOICE (ESTIMATED)")
    print("="*50)
    print(f"Prompt Tokens:    {total_prompt_tokens:,}")
    print(f"Candidate Tokens: {total_candidate_tokens:,}")
    print("-" * 50)
    print(f"LIKELY CHARGE:    ${likely_cost:.4f}")
    print("="*50)

if __name__ == "__main__":
    harvest_results()
