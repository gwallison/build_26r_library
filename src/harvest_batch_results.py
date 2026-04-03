# -*- coding: utf-8 -*-
"""
harvest_batch_results.py
-------------------------
Downloads the JSONL output from a completed Gemini Batch Job,
parses the internal CSV structure (SAMPLES, RESULTS, QUALIFIERS),
and saves consolidated parquet files.
"""

import os
import sys
import json
import io
import pandas as pd
from google import genai

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
JOB_FILE = os.path.join("data", "current_batch_job.txt")
OUTPUT_DIR = os.path.join("data", "output", "batch_harvest")

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def parse_csv_sections(text):
    """Parses the custom ### SECTION format into dataframes."""
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
        try:
            # Skip the 'Columns:' helper lines if the model included them
            clean_lines = [l for l in csv_data.splitlines() if not l.strip().startswith("- ") and "Columns:" not in l]
            if not clean_lines:
                continue
            
            df = pd.read_csv(io.StringIO("\n".join(clean_lines)), quotechar='"', skipinitialspace=True)
            dfs[name] = df
        except Exception as e:
            print(f"  Warning: Could not parse section {name}: {e}")
            
    return dfs

def harvest_results():
    if not os.path.exists(JOB_FILE):
        print(f"No job ID found in {JOB_FILE}")
        return

    with open(JOB_FILE, "r") as f:
        job_id = f.read().strip()

    print(f"Fetching job details for: {job_id}")
    job = client.batches.get(name=job_id)

    if job.state.name != "JOB_STATE_SUCCEEDED":
        print(f"Job is not finished. Current state: {job.state.name}")
        return

    # The output file name is usually in job.dest or check metadata
    # For Google AI (non-Vertex), the output is often a file name like "files/..."
    output_file_name = job.dest
    if not output_file_name:
        print("Error: No output destination found in job record.")
        return

    print(f"Downloading results from: {output_file_name}")
    try:
        content_bytes = client.files.download(name=output_file_name)
        # JSONL content
        lines = content_bytes.decode('utf-8').splitlines()
    except Exception as e:
        print(f"Error downloading output file: {e}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_samples = []
    all_results = []
    all_qualifiers = []

    print(f"Parsing {len(lines)} result lines...")
    for i, line in enumerate(lines):
        try:
            data = json.loads(line)
            # The structure for Batch API output:
            # {"response": {"candidates": [{"content": {"parts": [{"text": "..."}]}}]}}
            
            response = data.get("response", {})
            candidates = response.get("candidates", [])
            if not candidates:
                print(f"  Line {i}: No candidates found.")
                continue
                
            text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if not text:
                print(f"  Line {i}: No text in response.")
                continue

            # Parse the text into sections
            sections = parse_csv_sections(text)
            
            if "SAMPLES" in sections:
                all_samples.append(sections["SAMPLES"])
            if "RESULTS" in sections:
                all_results.append(sections["RESULTS"])
            if "QUALIFIERS" in sections:
                all_qualifiers.append(sections["QUALIFIERS"])
                
        except Exception as e:
            print(f"  Error parsing line {i}: {e}")

    # Consolidate and Save
    if all_samples:
        df_samples = pd.concat(all_samples, ignore_index=True)
        df_samples.to_parquet(os.path.join(OUTPUT_DIR, "batch_harvest_samples.parquet"), index=False)
        print(f"Saved {len(df_samples)} samples.")

    if all_results:
        df_results = pd.concat(all_results, ignore_index=True)
        df_results.to_parquet(os.path.join(OUTPUT_DIR, "batch_harvest_results.parquet"), index=False)
        print(f"Saved {len(df_results)} results.")

    if all_qualifiers:
        # Drop duplicates for qualifiers since they are often repeated
        df_qualifiers = pd.concat(all_qualifiers, ignore_index=True).drop_duplicates()
        df_qualifiers.to_parquet(os.path.join(OUTPUT_DIR, "batch_harvest_qualifiers.parquet"), index=False)
        print(f"Saved {len(df_qualifiers)} unique qualifiers.")

    print(f"\nHarvesting complete. Files saved to: {OUTPUT_DIR}")

if __name__ == "__main__":
    harvest_results()
