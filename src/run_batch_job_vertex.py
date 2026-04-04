# -*- coding: utf-8 -*-
"""
run_batch_job_vertex.py
-----------------------
Submits a batch job to Vertex AI using the generated JSONL file.
Requires the JSONL to be uploaded to GCS first.
"""

import os
import sys
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
# Ensure we use the correct model for Vertex
MODEL_NAME = "gemini-2.5-flash"

INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_vertex_100.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs/"

# Initializing Vertex AI client
client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location=LOCATION
)

def submit_batch():
    print(f"Submitting Vertex AI batch job for model {MODEL_NAME}...")
    print(f"Input: {INPUT_URI}")
    print(f"Output: {OUTPUT_URI}")
    
    try:
        job = client.batches.create(
            model=MODEL_NAME,
            src=INPUT_URI,
            config=types.CreateBatchJobConfig(
                dest=OUTPUT_URI
            )
        )

        print("\nBatch job submitted successfully!")
        print(f"Job ID: {job.name}")
        print(f"State: {job.state}")
        print(f"Created: {job.create_time}")

        # Save Job ID for tracking
        with open("data/current_batch_job_vertex.txt", "w") as f:
            f.write(job.name)

        return job.name
    except Exception as e:
        print(f"Error submitting batch job: {e}")
        return None

if __name__ == "__main__":
    submit_batch()
