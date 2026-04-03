# -*- coding: utf-8 -*-
"""
check_batch_status.py
---------------------
Checks the status of the current Gemini batch job.
"""

import os
import sys
from google import genai

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
JOB_FILE = "data/current_batch_job.txt"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def check_status():
    if not os.path.exists(JOB_FILE):
        print(f"No job ID found in {JOB_FILE}")
        return

    with open(JOB_FILE, "r") as f:
        job_id = f.read().strip()

    print(f"Checking status for job: {job_id}")

    try:
        job = client.batches.get(name=job_id)
        
        print(f"\nState: {job.state}")
        print(f"Created: {job.create_time}")
        print(f"Updated: {job.update_time}")
        
        if job.state.name == "JOB_STATE_SUCCEEDED":
            print(f"Output: {job.dest}")
        elif job.state.name == "JOB_STATE_FAILED":
            print(f"Error: {job.error}")
            
    except Exception as e:
        print(f"Error checking status: {e}")

if __name__ == "__main__":
    check_status()
