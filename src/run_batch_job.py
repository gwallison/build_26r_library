# -*- coding: utf-8 -*-
"""
run_batch_job.py
----------------
Submits a batch job to Gemini using the generated JSONL file.
Requires the JSONL to be uploaded to GCS.
"""

import os
import sys
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
MODEL_NAME = "gemini-3-flash-preview"
# UPDATE THESE with your GCS paths
INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_100.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs/"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)
def submit_batch():
    # 1. Upload the JSONL file to Gemini File API
    jsonl_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'batch_input_100.jsonl')
    print(f"Uploading {jsonl_path} to Gemini File API...")

    try:
        input_file = client.files.upload(
            file=jsonl_path,
            config=types.UploadFileConfig(
                display_name="batch-input-100",
                mime_type="application/jsonl"
            )
        )
        print(f"File uploaded successfully: {input_file.name}")
    except Exception as e:
        print(f"Error uploading JSONL file: {e}")
        return None

    # 2. Create the batch job using the file name
    print(f"Submitting batch job for model {MODEL_NAME}...")
    try:
        job = client.batches.create(
            model=MODEL_NAME,
            src=input_file.name
        )

        print("\nBatch job submitted successfully!")
        print(f"Job ID: {job.name}")
        print(f"State: {job.state}")
        print(f"Created: {job.create_time}")

        # Save Job ID for tracking
        with open("data/current_batch_job.txt", "w") as f:
            f.write(job.name)

        return job.name
    except Exception as e:
        print(f"Error submitting batch job: {e}")
        return None

if __name__ == "__main__":
    submit_batch()
