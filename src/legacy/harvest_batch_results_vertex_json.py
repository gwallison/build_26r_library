# -*- coding: utf-8 -*-
"""
harvest_batch_results_vertex_json.py
------------------------------------
Downloads and parses structured JSON Vertex AI Batch results.
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
JOB_FILE = os.path.join("data", "current_batch_job_vertex_json.txt")
OUTPUT_DIR = os.path.join("data", "output", "batch_harvest_vertex_json")

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

def harvest_results():
    if not os.path.exists(JOB_FILE):
        print(f"No job ID found in {JOB_FILE}")
        return

    with open(JOB_FILE, "r") as f:
        job_id = f.read().strip()

    print(f"Fetching job details: {job_id}")
    job = client.batches.get(name=job_id)

    if job.state.name != "JOB_STATE_SUCCEEDED":
        print(f"Job state: {job.state.name}")
        return

    dest_uri = job.dest.gcs_uri
    print(f"Output location: {dest_uri}")

    parts = dest_uri.replace("gs://", "").split("/")
    bucket_name = parts[0]
    prefix = "/".join(parts[1:])
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    
    # List objects with the prefix to find the timestamped subfolder
    blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
    jsonl_blobs = [b for b in blobs if "predictions.jsonl" in b.name]

    if not jsonl_blobs:
        print("No predictions found.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_samples = []
    all_results = []
    all_qualifiers = []
    
    total_prompt_tokens = 0
    total_response_tokens = 0

    for blob in jsonl_blobs:
        print(f"Processing {blob.name}...")
        content = blob.download_as_text()
        
        # Save raw JSONL for inspection
        raw_filename = blob.name.replace("/", "_").replace(":", "_")
        raw_path = os.path.join(OUTPUT_DIR, raw_filename)
        with open(raw_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  Saved raw output to: {raw_path}")
        
        for line in content.splitlines():
            try:
                data = json.loads(line)
                
                # Metadata
                usage = data.get("response", {}).get("usageMetadata", {})
                total_prompt_tokens += usage.get("promptTokenCount", 0)
                total_response_tokens += usage.get("candidatesTokenCount", 0)
                
                # Try to extract filename from request
                source_file = data.get("id", "unknown")
                
                # Parse JSON body
                candidates = data.get("response", {}).get("candidates", [])
                if not candidates: continue
                
                text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                if not text: continue
                
                # The structured output should be a valid JSON string already
                payload = json.loads(text)
                
                # Process Samples
                for s in payload.get("samples", []):
                    s["source_file"] = source_file
                    all_samples.append(s)
                    
                # Process Results
                for r in payload.get("results", []):
                    r["source_file"] = source_file
                    all_results.append(r)
                    
                # Process Qualifiers
                for q in payload.get("qualifiers", []):
                    q["source_file"] = source_file
                    all_qualifiers.append(q)
                    
            except Exception as e:
                print(f"  Error parsing line: {e}")

    # Save to Parquet
    if all_samples:
        pd.DataFrame(all_samples).to_parquet(os.path.join(OUTPUT_DIR, "samples.parquet"), index=False)
        print(f"Saved {len(all_samples)} samples.")
    if all_results:
        pd.DataFrame(all_results).to_parquet(os.path.join(OUTPUT_DIR, "results.parquet"), index=False)
        print(f"Saved {len(all_results)} results.")

    print("\n" + "="*30)
    print("TOKEN USAGE SUMMARY (JSON)")
    print("="*30)
    print(f"Total Prompt Tokens:    {total_prompt_tokens:,}")
    print(f"Total Response Tokens:  {total_response_tokens:,}")
    est_cost = (total_prompt_tokens * 0.10 / 1_000_000) + (total_response_tokens * 0.40 / 1_000_000)
    print(f"Estimated Cost:         ${est_cost:.4f}")
    print("="*30)

if __name__ == "__main__":
    harvest_results()
