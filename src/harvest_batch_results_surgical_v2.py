# -*- coding: utf-8 -*-
"""
harvest_batch_results_surgical_v2.py
------------------------------------
Downloads and parses the SURGICAL chunked Vertex AI Batch results.
Restores original filenames and page numbers using data/output/chunk_map.parquet.
Attaches Form 26R metadata via proximity join in post-processing.
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
JOB_ID_FILE = "data/current_batch_job_surgical_v2.txt"
OUTPUT_DIR = "data/output/batch_harvest_surgical_v2"

MAP_PATH = "data/output/chunk_map.parquet"
F26R_PATH = "data/output/all_harvested_form26r.parquet"
PROCESSED_TRACKER_PATH = "data/output/processed_files.parquet"

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
    "p": "chunk_page"
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

    # Load Mapping and Metadata
    print("Loading page map and F26R metadata...")
    chunk_map = pd.read_parquet(MAP_PATH)
    f26r = pd.read_parquet(F26R_PATH).sort_values(['filename', 'page_number'])

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

        with open(local_file, "r", encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    usage = data.get('response', {}).get('usageMetadata', {})
                    current_job_prompt_tokens += usage.get('promptTokenCount', 0)
                    current_job_response_tokens += usage.get('candidatesTokenCount', 0)

                    chunk_filename = data.get('id') # ID was the chunk filename
                    
                    if 'response' in data and 'candidates' in data['response']:
                        content = data['response']['candidates'][0]['content']['parts'][0]['text']
                        payload = json.loads(content)

                        for s in payload.get('samples', []):
                            sample_flat = {KEY_MAP.get(k, k): v for k, v in s.items()}
                            sample_flat['chunk_filename'] = chunk_filename
                            all_samples.append(sample_flat)

                        for r in payload.get('results', []):
                            result_flat = {KEY_MAP.get(k, k): v for k, v in r.items()}
                            result_flat['chunk_filename'] = chunk_filename
                            all_results.append(result_flat)
                except Exception:
                    pass

    if not all_results:
        print("No results found to harvest.")
        return

    print(f"Extracted {len(all_results)} raw result rows. Starting mapping...")

    # Create DataFrames
    df_res = pd.DataFrame(all_results)
    df_samp = pd.DataFrame(all_samples)

    # 1. Map Chunk Page to Original Page
    # Ensure chunk_page is numeric
    df_res['chunk_page'] = pd.to_numeric(df_res['chunk_page'], errors='coerce')
    
    df_merged = pd.merge(
        df_res, 
        chunk_map, 
        on=['chunk_filename', 'chunk_page'], 
        how='left'
    )

    # 2. Attach Sample Metadata (Matrix, Dates) back to results
    if not df_samp.empty:
        df_merged = pd.merge(
            df_merged, 
            df_samp[['lab_sample_id', 'chunk_filename', 'received_date', 'collection_date', 'matrix', 'is_poor_scan']],
            on=['lab_sample_id', 'chunk_filename'],
            how='left'
        )

    # 3. Proximity Join with Form 26R Metadata
    print("Performing proximity join with Form 26R context...")
    
    def get_f26r_context(row):
        if pd.isna(row['original_filename']) or pd.isna(row['original_page']):
            return pd.Series([None]*4)
        
        # Filter F26R to same file
        file_f26r = f26r[f26r['filename'] == row['original_filename']]
        # Preceding F26R (on or before the lab result page)
        preceding = file_f26r[file_f26r['page_number'] <= row['original_page']]
        
        if preceding.empty:
            return pd.Series([None]*4)
        
        last_f26r = preceding.iloc[-1]
        return pd.Series([
            last_f26r['company_name'],
            last_f26r['waste_location'],
            last_f26r['waste_code'],
            last_f26r['date_prepared']
        ])

    new_cols = ['f26r_company', 'f26r_location', 'f26r_waste_code', 'f26r_date_prepared']
    df_merged[new_cols] = df_merged.apply(get_f26r_context, axis=1)

    # Save Results
    output_path = os.path.join(OUTPUT_DIR, "results_v2.parquet")
    df_merged.to_parquet(output_path, index=False)
    print(f"Saved {len(df_merged)} results to {output_path}")

    # 4. Update Processed Tracker
    if os.path.exists(PROCESSED_TRACKER_PATH):
        tracker = pd.read_parquet(PROCESSED_TRACKER_PATH)
    else:
        tracker = pd.DataFrame(columns=['filename', 'status'])
    
    finished_files = df_merged['original_filename'].dropna().unique()
    new_entries = pd.DataFrame({'filename': finished_files, 'status': 'succeeded'})
    
    updated_tracker = pd.concat([tracker, new_entries]).drop_duplicates(subset=['filename'], keep='last')
    updated_tracker.to_parquet(PROCESSED_TRACKER_PATH, index=False)
    print(f"Updated tracker: {len(updated_tracker)} files now marked as processed.")

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
