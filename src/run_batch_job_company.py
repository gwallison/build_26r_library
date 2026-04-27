# -*- coding: utf-8 -*-
"""
run_batch_job_company.py
-------------------------
Submits the company name extraction batch job to Vertex AI.
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
MODEL_NAME = "gemini-1.5-flash-002"

# Local path for estimation
LOCAL_INPUT_JSONL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "batch_input_company.jsonl")

# GCS Paths
INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_company.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs-company/"

def submit_batch(auto_approve=False):
    # 1. Estimate cost first
    from estimate_batch_cost import estimate_cost, print_estimation
    
    print(f"Gathering cost estimate for company extraction run...")
    results = estimate_cost(LOCAL_INPUT_JSONL)
    if results:
        # Note: estimate_cost expects files to be in data/chunked_pdfs for page counting.
        # But for our company pages, they are in data/company_pages.
        # I'll monkeypatch the dir for this run.
        import estimate_batch_cost
        original_dir = estimate_batch_cost.CHUNKED_PDFS_DIR
        estimate_batch_cost.CHUNKED_PDFS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "company_pages")
        results = estimate_cost(LOCAL_INPUT_JSONL)
        estimate_batch_cost.CHUNKED_PDFS_DIR = original_dir
        
        print_estimation(results)
        
        price_key = "flash" # gemini-1.5-flash
        if price_key in results["models"]:
            est_total = results["models"][price_key]["total_cost"]
            print(f"\nEstimated cost for {MODEL_NAME}: ${est_total:,.2f}")
        
        if not auto_approve:
            confirm = input(f"\nProceed with batch submission to {MODEL_NAME}? (y/n): ")
            if confirm.lower() != 'y':
                print("Submission cancelled by user.")
                return None
        else:
            print(f"\nAuto-approving submission to {MODEL_NAME}...")
    else:
        print(f"Warning: Could not find local input file {LOCAL_INPUT_JSONL} for cost estimation.")
        if not auto_approve:
            confirm = input("Proceed without estimation? (y/n): ")
            if confirm.lower() != 'y':
                return None

    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

    print(f"Submitting company extraction batch job using {MODEL_NAME}...")
    print(f"Input:  {INPUT_URI}")
    print(f"Output: {OUTPUT_URI}")

    try:
        job = client.batches.create(
            model=MODEL_NAME,
            src=INPUT_URI,
            config=types.CreateBatchJobConfig(
                dest=OUTPUT_URI
            )
        )

        print(f"SUCCESS! Batch job created: {job.name}")
        
        # Save job ID for status checking
        with open("data/current_batch_job_company.txt", "w") as f:
            f.write(job.name)

        return job.name
    except Exception as e:
        print(f"Error submitting batch job: {e}")
        return None

if __name__ == "__main__":
    auto_approve = "--yes" in sys.argv
    submit_batch(auto_approve=auto_approve)
