# -*- coding: utf-8 -*-
"""
harvest_batch_results_surgical_v2.py
------------------------------------
Downloads and parses the SURGICAL chunked Vertex AI Batch results (from Physical Micro-PDFs).
Restores the original filenames from the embedded IDs and calculates usage cost.
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
JOB_ID_FILE = "data/current_batch_job_surgical_v2.txt"
OUTPUT_DIR = "data/output/batch_harvest_surgical_v2"

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
            print(f"  Downloaded: {subfolder}/{os.path.basename(blob.name)}")

        is_current = blob.time_created >= job.create_time

        with open(local_file, "r", encoding='utf-8') as f:
            for line_idx, line in enumerate(f):
                try:
                    data = json.loads(line)
                    
                    if is_current:
                        usage = data.get('response', {}).get('usageMetadata', {})
                        current_job_prompt_tokens += usage.get('promptTokenCount', 0)
                        current_job_response_tokens += usage.get('candidatesTokenCount', 0)

                        # Parse ID: original_fn|||chunk_file
                        request_id = data.get('id', 'unknown|||unknown')
                        parts = request_id.split('|||')
                        original_filename = parts[0] if len(parts) > 1 else request_id
                        
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
                                    "filename": original_filename # Restored!
                                })
                                all_samples.append(sample_flat)

                            for r in payload.get('results', []):
                                result_flat = {KEY_MAP.get(k, k): v for k, v in r.items()}
                                result_flat['filename'] = original_filename # Restored!
                                all_results.append(result_flat)
                except Exception as e:
                    pass

    # Save to Parquet
    if all_samples:
        df_samples = pd.DataFrame(all_samples)
        df_samples.to_parquet(os.path.join(OUTPUT_DIR, "samples_v2.parquet"), index=False)
        print(f"Saved {len(df_samples)} samples.")

    if all_results:
        df_results = pd.DataFrame(all_results)
        df_results.to_parquet(os.path.join(OUTPUT_DIR, "results_v2.parquet"), index=False)
        print(f"Saved {len(df_results)} results.")

    print("\n" + "="*40)
    print("USAGE SUMMARY: V2 PHYSICAL CHUNKING JOB")
    print("="*40)
    print(f"Prompt Tokens:   {current_job_prompt_tokens:,}")
    print(f"Response Tokens: {current_job_response_tokens:,}")
    cost_cur = (current_job_prompt_tokens * 0.10 / 1_000_000) + (current_job_response_tokens * 0.40 / 1_000_000)
    print(f"Estimated Cost:  ${cost_cur:.4f}")
    print("="*40)

if __name__ == "__main__":
    harvest_results()
