# -*- coding: utf-8 -*-
"""
harvest_batch_results_surgical_v1.py
------------------------------------
Downloads and parses the SURGICAL chunked Vertex AI Batch results.
Expands short keys back to full names and merges chunks into unified Parquet files.
"""

import os
import sys
import json
import pandas as pd
from google import genai
from google.cloud import storage

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
JOB_ID_FILE = "data/current_batch_job_surgical_v1.txt"
OUTPUT_DIR = "data/output/batch_harvest_surgical_v1"

# Key mapping: Surgical -> Canonical
KEY_MAP = {
    "sid": "lab_sample_id",
    "cid": "client_sample_id",
    "rd": "received_date",
    "cd": "collection_date",
    "m": "matrix",
    "bad": "is_poor_scan",
    "a": "analyte",
    "r": "result",
    "rl": "reporting_limit",
    "mdl": "mdl",
    "u": "units",
    "q": "qualifier_code",
    "p": "pdf_page_number"
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

    # Initialize GCS client
    storage_client = storage.Client(project=PROJECT_ID)
    bucket_name = dest_uri.replace("gs://", "").split("/")[0]
    prefix = "/".join(dest_uri.replace("gs://", "").split("/")[1:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_samples = []
    all_results = []
    
    current_job_prompt_tokens = 0
    current_job_response_tokens = 0
    
    cum_prompt_tokens = 0
    cum_response_tokens = 0

    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    jsonl_blobs = [b for b in blobs if b.name.endswith("predictions.jsonl")]
    
    # We identify the 'current job' blobs by looking for the most recent 
    # subfolder or matching timestamp. However, since we have the 'job' 
    # object, we can try to find the specific output path.
    # Vertex usually creates: prefix/prediction-model-TIMESTAMP/predictions.jsonl
    
    print(f"Found {len(jsonl_blobs)} total prediction files in the parent directory.")

    for blob in jsonl_blobs:
        # Create a unique local path to avoid collisions
        # Sanitize name for Windows (remove colons from timestamps)
        subfolder = os.path.basename(os.path.dirname(blob.name)).replace(":", "_")
        local_job_dir = os.path.join(OUTPUT_DIR, subfolder)
        os.makedirs(local_job_dir, exist_ok=True)
        
        local_file = os.path.join(local_job_dir, os.path.basename(blob.name))
        
        # Only download if not already present, to save time
        if not os.path.exists(local_file):
            blob.download_to_filename(local_file)
            print(f"  Downloaded: {subfolder}/{os.path.basename(blob.name)}")

        # Is this the CURRENT job? 
        # Check if the blob was created AFTER the job started
        is_current = blob.time_created >= job.create_time

        with open(local_file, "r", encoding='utf-8') as f:
            for line_idx, line in enumerate(f):
                try:
                    data = json.loads(line)
                    usage = data.get('response', {}).get('usageMetadata', {})
                    p_tokens = usage.get('promptTokenCount', 0)
                    r_tokens = usage.get('candidatesTokenCount', 0)
                    
                    cum_prompt_tokens += p_tokens
                    cum_response_tokens += r_tokens
                    
                    if is_current:
                        current_job_prompt_tokens += p_tokens
                        current_job_response_tokens += r_tokens

                    # Process data only for the current job to avoid duplicates in Parquet
                    if is_current:
                        # Extract ID
                        request_id = data.get('id', 'unknown')
                        filename = request_id.replace("gs://fta-form26r-library/full-set/", "").split("_chunk_")[0]

                        # Get response text
                        if 'response' in data and 'candidates' in data['response']:
                            content = data['response']['candidates'][0]['content']['parts'][0]['text']
                            payload = json.loads(content)

                            meta = payload.get('meta', {})
                            for s in payload.get('samples', []):
                                sample_flat = {KEY_MAP.get(k, k): v for k, v in s.items()}
                                sample_flat.update({
                                    "lab_report_id": meta.get("rid"),
                                    "lab_name": meta.get("ln"),
                                    "client_name": meta.get("cn"),
                                    "f26r_company": meta.get("f_co"),
                                    "f26r_location": meta.get("f_loc"),
                                    "f26r_waste_code": meta.get("f_code"),
                                    "f26r_date_prepared": meta.get("f_dt"),
                                    "filename": filename
                                })
                                all_samples.append(sample_flat)

                            for r in payload.get('results', []):
                                result_flat = {KEY_MAP.get(k, k): v for k, v in r.items()}
                                result_flat['filename'] = filename
                                all_results.append(result_flat)
                except:
                    pass

    # Save CURRENT job results to Parquet
    if all_samples:
        df_samples = pd.DataFrame(all_samples)
        df_samples.to_parquet(os.path.join(OUTPUT_DIR, "samples_current.parquet"), index=False)
        print(f"Saved {len(df_samples)} samples from CURRENT job.")

    if all_results:
        df_results = pd.DataFrame(all_results)
        df_results.to_parquet(os.path.join(OUTPUT_DIR, "results_current.parquet"), index=False)
        print(f"Saved {len(df_results)} results from CURRENT job.")

    print("\n" + "="*40)
    print("USAGE SUMMARY: CURRENT JOB")
    print("="*40)
    print(f"Prompt Tokens:   {current_job_prompt_tokens:,}")
    print(f"Response Tokens: {current_job_response_tokens:,}")
    cost_cur = (current_job_prompt_tokens * 0.10 / 1_000_000) + (current_job_response_tokens * 0.40 / 1_000_000)
    print(f"Estimated Cost:  ${cost_cur:.4f}")

    print("\n" + "="*40)
    print("USAGE SUMMARY: CUMULATIVE (All Jobs)")
    print("="*40)
    print(f"Prompt Tokens:   {cum_prompt_tokens:,}")
    print(f"Response Tokens: {cum_response_tokens:,}")
    cost_cum = (cum_prompt_tokens * 0.10 / 1_000_000) + (cum_response_tokens * 0.40 / 1_000_000)
    print(f"Estimated Cost:  ${cost_cum:.4f}")
    print("="*40)

if __name__ == "__main__":
    harvest_results()
