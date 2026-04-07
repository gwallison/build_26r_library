# -*- coding: utf-8 -*-
"""
check_batch_status.py
---------------------
Checks the status of the current Gemini batch job (Google AI OR Vertex AI).
"""

import os
import sys
from google import genai

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"

# We check for the V2 surgical job file first, then the legacy one
JOB_FILES = [
    "data/current_batch_job_surgical_v2.txt",
    "data/current_batch_job.txt"
]

def check_status():
    job_id = None
    active_file = None
    
    for jf in JOB_FILES:
        if os.path.exists(jf):
            with open(jf, "r") as f:
                job_id = f.read().strip()
                active_file = jf
                break
                
    if not job_id:
        print("No job ID found in any tracker files.")
        return

    print(f"Checking status from {active_file}...")
    print(f"Job ID: {job_id}")

    try:
        # Determine if it's a Vertex ID or a Gemini AI ID
        if "projects/" in job_id or "locations/" in job_id:
            # Vertex AI
            client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
        else:
            # Google AI (Flash Batch)
            api_key = os.getenv("GoogleAI-API-key")
            client = genai.Client(api_key=api_key)
            
        job = client.batches.get(name=job_id)
        
        print(f"\nState: {job.state.name if hasattr(job.state, 'name') else job.state}")
        print(f"Created: {job.create_time}")
        
        if hasattr(job, 'update_time'):
            print(f"Updated: {job.update_time}")
        
        if job.state.name == "JOB_STATE_SUCCEEDED":
            print("Job complete! Run the harvester to download results.")
        elif job.state.name == "JOB_STATE_FAILED":
            print(f"Error: {getattr(job, 'error', 'Unknown Error')}")
            
    except Exception as e:
        print(f"Error checking status: {e}")

if __name__ == "__main__":
    check_status()
