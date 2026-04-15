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

# Gemini 2.5 Batch Pricing (50% discount applied)
PRICE_INPUT = 0.075 / 1_000_000
PRICE_OUTPUT = 0.30 / 1_000_000

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
    
    total_prompt_tokens = 0
    total_candidate_tokens = 0
    total_thought_tokens = 0

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
                    
                    # Track tokens from THIS specific job only (to avoid cumulative errors)
                    # We only add if the processed_time matches today's date roughly or if we want job-specific metrics
                    # For now, we sum everything in the harvested files
                    total_prompt_tokens += usage.get('promptTokenCount', 0)
                    total_candidate_tokens += usage.get('candidatesTokenCount', 0)
                    total_thought_tokens += usage.get('thoughtsTokenCount', 0)

                    # ID was the chunk filename, but might have ||| prefix
                    chunk_filename_raw = data.get('id', '')
                    if '|||' in chunk_filename_raw:
                        chunk_filename = chunk_filename_raw.split('|||')[-1]
                    else:
                        chunk_filename = chunk_filename_raw
                    
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
    df_res['chunk_page'] = pd.to_numeric(df_res['chunk_page'], errors='coerce')
    map_cols = ['chunk_filename', 'chunk_page', 'original_page', 'original_filename']
    if 'set_name' in chunk_map.columns:
        map_cols.append('set_name')
        
    df_merged = pd.merge(
        df_res, 
        chunk_map[map_cols], 
        on=['chunk_filename', 'chunk_page'], 
        how='inner'
    )
    
    if 'set_name' not in df_merged.columns:
        triage = pd.read_parquet("data/output/lab_report_triage.parquet")[['set_name', 'filename']].drop_duplicates()
        df_merged = pd.merge(df_merged, triage, left_on='original_filename', right_on='filename', how='left')
        if 'filename' in df_merged.columns:
             df_merged = df_merged.drop(columns=['filename'])

    # 2. Attach Sample Metadata
    if not df_samp.empty:
        df_samp_clean = df_samp.sort_values(
            by=['lab_sample_id', 'chunk_filename', 'received_date', 'collection_date'], 
            ascending=[True, True, False, False]
        ).drop_duplicates(subset=['lab_sample_id', 'chunk_filename'], keep='first')
        
        df_merged = pd.merge(
            df_merged, 
            df_samp_clean[['lab_sample_id', 'chunk_filename', 'received_date', 'collection_date', 'matrix', 'is_poor_scan']],
            on=['lab_sample_id', 'chunk_filename'],
            how='left'
        )

    # 3. Proximity Join with Form 26R Metadata
    print("Performing proximity join with Form 26R context (robust strategy)...")
    
    # Ensure numeric types
    df_merged['original_page'] = pd.to_numeric(df_merged['original_page'], errors='coerce')
    f26r['page_number'] = pd.to_numeric(f26r['page_number'], errors='coerce')
    
    # 1. Broad merge on filename to get all candidate F26Rs for each result
    df_prox = pd.merge(
        df_merged,
        f26r[['filename', 'page_number', 'company_name', 'waste_location', 'waste_code', 'date_prepared']],
        left_on='original_filename',
        right_on='filename',
        how='left'
    )
    
    # 2. Filter: Only keep F26Rs that appear ON or BEFORE the current result page
    df_prox = df_prox[
        (df_prox['page_number'].isna()) | 
        (df_prox['page_number'] <= df_prox['original_page'])
    ]
    
    # 3. Select the LATEST F26R page for each result (the one closest to it)
    # We sort by page_number and keep the last one per unique result record
    df_prox = df_prox.sort_values('page_number').drop_duplicates(
        subset=['chunk_filename', 'lab_sample_id', 'analyte', 'original_page', 'result'], 
        keep='last'
    )
    
    # Rename columns to match expected output
    df_merged = df_prox.rename(columns={
        'company_name': 'f26r_company',
        'waste_location': 'f26r_location',
        'waste_code': 'f26r_waste_code',
        'date_prepared': 'f26r_date_prepared'
    }).drop(columns=['filename', 'page_number'], errors='ignore')

    # Save Results
    output_path = os.path.join(OUTPUT_DIR, "results_v2.parquet")
    df_merged = df_merged.drop_duplicates()
    df_merged.to_parquet(output_path, index=False)
    print(f"Saved {len(df_merged)} results to {output_path}")

    # 4. Update Processed Tracker
    if os.path.exists(PROCESSED_TRACKER_PATH):
        tracker = pd.read_parquet(PROCESSED_TRACKER_PATH)
    else:
        tracker = pd.DataFrame(columns=['set_name', 'filename', 'status'])
    
    finished_files = df_merged[['set_name', 'original_filename']].dropna().drop_duplicates()
    finished_files.columns = ['set_name', 'filename']
    finished_files['status'] = 'succeeded'
    
    updated_tracker = pd.concat([tracker, finished_files]).drop_duplicates(subset=['set_name', 'filename'], keep='last')
    updated_tracker.to_parquet(PROCESSED_TRACKER_PATH, index=False)

    # Final Detailed Cost Summary
    # Note: Prompt + Candidate are generally billable. Thoughts are currently free.
    likely_billable_tokens = total_prompt_tokens + total_candidate_tokens
    max_tokens = likely_billable_tokens + total_thought_tokens
    
    likely_cost = (total_prompt_tokens * PRICE_INPUT) + (total_candidate_tokens * PRICE_OUTPUT)
    max_cost = (total_prompt_tokens * PRICE_INPUT) + ((total_candidate_tokens + total_thought_tokens) * PRICE_OUTPUT)

    print("\n" + "="*50)
    print("DETAILED COST INVOICE (ESTIMATED)")
    print("="*50)
    print(f"Prompt Tokens (Billable):    {total_prompt_tokens:,}")
    print(f"Candidate Tokens (Billable): {total_candidate_tokens:,}")
    print(f"Thought Tokens (FREE):       {total_thought_tokens:,}")
    print("-" * 50)
    print(f"LIKELY CHARGE:               ${likely_cost:.4f}")
    print(f"MAX POSSIBLE CHARGE:         ${max_cost:.4f}")
    print("="*50)
    print(f"Total processed documents in this tracker: {len(updated_tracker)}")

if __name__ == "__main__":
    harvest_results()
