# -*- coding: utf-8 -*-
"""
check_batch_status_vertex.py
----------------------------
Checks the status of the current Vertex AI batch job.
"""

import os
import sys
from google import genai

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
JOB_FILE = "data/current_batch_job_vertex.txt"

# Initializing Vertex AI client
client = genai.Client(
    vertexai=True,
    project=PROJECT_ID,
    location=LOCATION
)

def check_status():
    job_file = JOB_FILE
    if len(sys.argv) > 1:
        job_file = sys.argv[1]
        
    if not os.path.exists(job_file):
        print(f"No job ID found in {job_file}")
        return

    with open(job_file, "r") as f:
        job_id = f.read().strip()

    print(f"Checking Vertex status for job: {job_id}")

    try:
        job = client.batches.get(name=job_id)
        
        print(f"\nState: {job.state}")
        print(f"Created: {job.create_time}")
        print(f"Updated: {job.update_time}")
        
        if job.state.name == "JOB_STATE_SUCCEEDED":
            print(f"Output Destination: {job.dest}")
        elif job.state.name == "JOB_STATE_FAILED":
            print(f"Error: {job.error}")
            
    except Exception as e:
        print(f"Error checking status: {e}")

if __name__ == "__main__":
    check_status()
