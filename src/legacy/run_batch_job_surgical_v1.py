# -*- coding: utf-8 -*-
"""
run_batch_job_surgical_v1.py
----------------------------
Submits the SURGICAL chunked batch job to Vertex AI.
"""

import os
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
MODEL_NAME = "gemini-1.5-flash"

INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_surgical_v1.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs-surgical/"

def submit_batch():
    client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

    print(f"Submitting surgical batch job using {MODEL_NAME}...")
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
        with open("data/current_batch_job_surgical_v1.txt", "w") as f:
            f.write(job.name)

        return job.name
    except Exception as e:
        print(f"Error submitting batch job: {e}")
        return None

if __name__ == "__main__":
    submit_batch()
