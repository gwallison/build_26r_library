# -*- coding: utf-8 -*-
"""
harvest_batch_results_vertex.py
-------------------------------
Downloads and parses Vertex AI Batch results from GCS.
Saves extracted data to Parquet files.
"""

import os
import sys
import json
import io
import pandas as pd
from google import genai
from google.cloud import storage

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
JOB_FILE = os.path.join("data", "current_batch_job_vertex.txt")
OUTPUT_DIR = os.path.join("data", "output", "batch_harvest_vertex")

CANONICAL_HEADERS = {
    "SAMPLES": [
        "lab_report_id", "lab_name", "client_name", "received_date", 
        "client_sample_id", "lab_sample_id", "collection_date", "matrix", 
        "sample_notes", "extraction_notes", "f26r_company_name", 
        "f26r_waste_location", "f26r_waste_code", "f26r_date_prepared"
    ],
    "RESULTS": [
        "lab_sample_id", "analyte", "result", "reporting_limit", "mdl", 
        "units", "qualifier_code", "dilution_factor", "analysis_date", 
        "method", "pdf_page_number"
    ],
    "QUALIFIERS": ["qualifier_code", "description"]
}

# Initializing Vertex AI client
client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location=LOCATION
)

import re
import csv

def parse_csv_sections(text):
    """Parses the custom ### SECTION format into dataframes using a resilient line-by-line approach."""
    sections = {}
    current_section = None
    current_lines = []

    for line in text.splitlines():
        if line.startswith("### "):
            if current_section:
                sections[current_section] = "\n".join(current_lines)
            current_section = line.replace("### ", "").strip()
            current_lines = []
        elif current_section:
            current_lines.append(line)
            
    if current_section:
        sections[current_section] = "\n".join(current_lines)
        
    dfs = {}
    for name, csv_data in sections.items():
        if name not in CANONICAL_HEADERS:
            continue
            
        try:
            # Skip helper lines and empty lines
            lines = [l for l in csv_data.splitlines() if l.strip() and not l.strip().startswith("- ") and "Columns:" not in l]
            if not lines:
                continue
            
            canonical_cols = CANONICAL_HEADERS[name]
            expected_count = len(canonical_cols)
            
            # Resilient line-by-line parsing
            parsed_rows = []
            
            # Check if first line is a header
            first_line = lines[0]
            reader = csv.reader([first_line], quotechar='"', skipinitialspace=True)
            first_row = next(reader)
            
            start_idx = 0
            if any(term.lower() in [val.lower() for val in first_row] for term in canonical_cols):
                start_idx = 1 # Skip header row
            
            for line in lines[start_idx:]:
                try:
                    # Parse single line
                    reader = csv.reader([line], quotechar='"', skipinitialspace=True)
                    row = next(reader)
                    
                    row_dict = {
                        "is_flagged": False,
                        "parsing_error": "",
                        "raw_csv_output": line
                    }
                    
                    # Fill canonical columns
                    actual_count = len(row)
                    if actual_count != expected_count:
                        row_dict["is_flagged"] = True
                        row_dict["parsing_error"] = f"Column count mismatch: expected {expected_count}, saw {actual_count}"
                    
                    # Map values to canonical columns, handling extras or missing
                    for i, col_name in enumerate(canonical_cols):
                        row_dict[col_name] = row[i] if i < actual_count else None
                    
                    # If there are extra columns, append them to a 'tail' field for diagnosis
                    if actual_count > expected_count:
                        row_dict["extra_data"] = " | ".join(row[expected_count:])
                    else:
                        row_dict["extra_data"] = ""
                        
                    parsed_rows.append(row_dict)
                    
                except Exception as line_err:
                    parsed_rows.append({
                        "is_flagged": True,
                        "parsing_error": f"Line-level parse error: {line_err}",
                        "raw_csv_output": line,
                        **{col: None for col in canonical_cols},
                        "extra_data": ""
                    })
            
            if parsed_rows:
                df = pd.DataFrame(parsed_rows)
                dfs[name] = df
                
        except Exception as e:
            print(f"  Critical error parsing section {name}: {e}")
            
    return dfs

def harvest_results():
    if not os.path.exists(JOB_FILE):
        print(f"No job ID found in {JOB_FILE}")
        return

    with open(JOB_FILE, "r") as f:
        job_id = f.read().strip()

    print(f"Fetching Vertex job details: {job_id}")
    job = client.batches.get(name=job_id)

    if job.state.name != "JOB_STATE_SUCCEEDED":
        print(f"Job not finished. State: {job.state.name}")
        return

    # Vertex dest is a BatchJobDestination object
    dest_uri = job.dest.gcs_uri
    if not dest_uri:
        print("Error: No GCS output destination found in job record.")
        return
        
    print(f"Output location: {dest_uri}")

    # Parse GCS URI
    parts = dest_uri.replace("gs://", "").split("/")
    bucket_name = parts[0]
    
    # Vertex job name is usually 'projects/.../locations/.../batchPredictionJobs/JOB_ID'
    # The output folder name usually starts with 'prediction-model-'
    # But job.dest often points to the ROOT.
    # We should look for blobs that are specifically related to this job's output folder if possible.
    # Actually, job.dest in SUCCEEDED state usually includes the full timestamped subfolder.
    prefix = "/".join(parts[1:])
    if not prefix.endswith("/"):
        prefix += "/"

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    
    # List all objects in the output directory
    blobs = list(storage_client.list_blobs(bucket, prefix=prefix))
    
    # Vertex usually names the output file something like 'predictions.jsonl-00000-of-00001'
    jsonl_blobs = [b for b in blobs if "predictions.jsonl" in b.name]
    
    if not jsonl_blobs:
        print("No prediction JSONL files found in destination.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_samples = []
    all_results = []
    all_qualifiers = []
    
    # Token usage counters
    total_prompt_tokens = 0
    total_response_tokens = 0
    total_cached_tokens = 0

    for blob in jsonl_blobs:
        print(f"Processing {blob.name}...")
        content = blob.download_as_text()
        
        # Save the raw JSONL for audit (sanitize filename for Windows)
        raw_filename = blob.name.replace("/", "_").replace(":", "_")
        raw_path = os.path.join(OUTPUT_DIR, raw_filename)
        with open(raw_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  Saved raw output to: {raw_path}")

        lines = content.splitlines()
        
        for i, line in enumerate(lines):
            try:
                data = json.loads(line)
                # Try to extract the source filename from 'id' or request parts
                source_file = data.get("id")
                
                if not source_file:
                    # BRUTE FORCE: Search the whole line for any 'full-set' GCS URI
                    # (Captures everything until the closing quote to handle spaces)
                    match = re.search(r'gs://fta-form26r-library/full-set/[^"]+', line)
                    if match:
                        source_file = match.group(0)
                    else:
                        source_file = "unknown_file"
                
                # Vertex format
                response = data.get("response", {})
                
                # Extract usage metadata
                usage = response.get("usageMetadata", {})
                total_prompt_tokens += usage.get("promptTokenCount", 0)
                total_response_tokens += usage.get("candidatesTokenCount", 0)
                # Note: Field name in raw JSON is often camelCase
                total_cached_tokens += usage.get("cachedContentTokenCount", 0)

                candidates = response.get("candidates", [])
                if not candidates: continue
                
                text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                if not text: continue

                sections = parse_csv_sections(text)
                
                for section_name, df in sections.items():
                    df["source_file"] = source_file
                    if section_name == "SAMPLES": all_samples.append(df)
                    if section_name == "RESULTS": all_results.append(df)
                    if section_name == "QUALIFIERS": all_qualifiers.append(df)
            except Exception as e:
                print(f"  Error line {i}: {e}")

    # Consolidate and Save
    if all_samples:
        df_samples = pd.concat(all_samples, ignore_index=True)
        df_samples.to_parquet(os.path.join(OUTPUT_DIR, "batch_harvest_samples_vertex.parquet"), index=False)
        print(f"Saved {len(df_samples)} samples.")

    if all_results:
        df_results = pd.concat(all_results, ignore_index=True)
        df_results.to_parquet(os.path.join(OUTPUT_DIR, "batch_harvest_results_vertex.parquet"), index=False)
        print(f"Saved {len(df_results)} results.")

    # Print Usage Summary
    print("\n" + "="*30)
    print("TOKEN USAGE SUMMARY")
    print("="*30)
    print(f"Total Prompt Tokens:    {total_prompt_tokens:,}")
    print(f"Total Response Tokens:  {total_response_tokens:,}")
    print(f"Total Cached Tokens:    {total_cached_tokens:,}")
    print(f"Total Tokens:           {total_prompt_tokens + total_response_tokens:,}")
    
    # Simple Cost Estimation (Gemini 2.5 Flash Batch prices - approx 50% discount)
    # Prompt: $0.10 / 1M tokens (batch)
    # Response: $0.40 / 1M tokens (batch)
    # (Note: These are estimates; check current GCP pricing for exact numbers)
    est_cost = (total_prompt_tokens * 0.10 / 1_000_000) + (total_response_tokens * 0.40 / 1_000_000)
    print(f"Estimated Cost:         ${est_cost:.4f}")
    print("="*30)

    print(f"\nHarvesting complete. Files in: {OUTPUT_DIR}")

if __name__ == "__main__":
    harvest_results()
