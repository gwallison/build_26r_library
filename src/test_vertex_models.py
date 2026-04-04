import os
from google import genai
from google.genai import types

PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
INPUT_URI = "gs://fta-form26r-library/batch-inputs/batch_input_vertex_100.jsonl"
OUTPUT_URI = "gs://fta-form26r-library/batch-outputs/"

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)

test_models = [
    "publishers/google/models/gemini-3-flash-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-2.0-flash-001",
    "gemini-1.5-flash-002"
]

for m in test_models:
    print(f"Testing model: {m}...")
    try:
        job = client.batches.create(
            model=m,
            src=INPUT_URI,
            config=types.CreateBatchJobConfig(
                dest=OUTPUT_URI
            )
        )
        print(f"  SUCCESS! Job created: {job.name}")
        # Stop at the first success
        break
    except Exception as e:
        print(f"  FAILED: {e}")
        print("-" * 20)
