# -*- coding: utf-8 -*-
"""
run_batch_job_surgical_v2.py
----------------------------
Submits the V2 SURGICAL chunked batch job to Vertex AI (Using physically split PDFs).
"""

import os
import sys
from google import genai
from google.genai import types

# Add the current script's directory to sys.path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-flash-lite"

INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_surgical_v2.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs-surgical-v2/"

# Local paths for estimation
LOCAL_INPUT_JSONL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "batch_input_surgical_v2.jsonl")
LOCAL_INPUT_JSONL_SAMPLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "batch_input_surgical_v2_sample.jsonl")

# GCS Paths
INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_surgical_v2.jsonl"
INPUT_URI_SAMPLE = "gs://fta-form26r-library/batch-inputs/batch_input_surgical_v2_sample.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs-surgical-v2/"

def submit_batch(is_sample=False, auto_approve=False):
    # 1. Estimate cost first
    from estimate_batch_cost import estimate_cost, print_estimation
    
    local_path = LOCAL_INPUT_JSONL_SAMPLE if is_sample else LOCAL_INPUT_JSONL
    gcs_input = INPUT_URI_SAMPLE if is_sample else INPUT_URI
    
    print(f"Gathering cost estimate for {'SAMPLE' if is_sample else 'FULL'} run...")
    results = estimate_cost(local_path)
    if results:
        print_estimation(results)
        
        # Check if the model we are using matches the estimate
        # Map full identifier to pricing key
        price_key = None
        if "flash-lite" in MODEL_NAME.lower(): price_key = "flash-lite"
        elif "flash" in MODEL_NAME.lower(): price_key = "flash"
        elif "pro" in MODEL_NAME.lower(): price_key = "pro"
        
        if price_key and price_key in results["models"]:
            est_total = results["models"][price_key]["total_cost"]
            print(f"\nEstimated cost for {MODEL_NAME}: ${est_total:,.2f}")
        else:
            print(f"\nWarning: Model {MODEL_NAME} tier not found in pricing map. Cannot confirm estimate.")
        
        if not auto_approve:
            confirm = input(f"\nProceed with batch submission to {MODEL_NAME}? (y/n): ")
            if confirm.lower() != 'y':
                print("Submission cancelled by user.")
                return None
        else:
            print(f"\nAuto-approving submission to {MODEL_NAME}...")
    else:
        print(f"Warning: Could not find local input file {local_path} for cost estimation.")
        if not auto_approve:
            confirm = input("Proceed without estimation? (y/n): ")
            if confirm.lower() != 'y':
                return None
        else:
            print("Auto-approving submission without estimation...")

    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

    print(f"Submitting V2 surgical {'SAMPLE' if is_sample else 'FULL'} batch job using {MODEL_NAME}...")
    print(f"Input:  {gcs_input}")
    print(f"Output: {OUTPUT_URI}")

    try:
        job = client.batches.create(
            model=MODEL_NAME,
            src=gcs_input,
            config=types.CreateBatchJobConfig(
                dest=OUTPUT_URI
            )
        )

        print(f"SUCCESS! Batch job created: {job.name}")
        
        # Save job ID for status checking
        with open("data/current_batch_job_surgical_v2.txt", "w") as f:
            f.write(job.name)

        return job.name
    except Exception as e:
        print(f"Error submitting batch job: {e}")
        return None

if __name__ == "__main__":
    is_sample = "--sample" in sys.argv
    auto_approve = "--yes" in sys.argv
    submit_batch(is_sample=is_sample, auto_approve=auto_approve)
