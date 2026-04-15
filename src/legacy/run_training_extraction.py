# -*- coding: utf-8 -*-
"""
run_training_extraction.py
--------------------------
Runs Gemini extraction on a single training PDF (e.g., from data/training_examples/).
Saves the result as a CSV in the same directory.

Usage:
    python src/run_training_extraction.py data/training_examples/training_filename.pdf
"""

import os
import sys
import time
from google import genai
from get_single_file_prompt import SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
MODEL_NAME = "gemini-3-flash-preview"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

# Initialize Client
client = genai.Client(api_key=API_KEY)

def run_training_extraction(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    filename = os.path.basename(file_path)
    output_dir = os.path.dirname(file_path)

    # 1. Prepare Output Filename
    # Change "training_" to "output_" and ".pdf" to ".csv"
    if filename.startswith("training_"):
        out_filename = filename.replace("training_", "output_", 1)
    else:
        out_filename = "output_" + filename
    
    out_filename = os.path.splitext(out_filename)[0] + ".csv"
    output_path = os.path.join(output_dir, out_filename)

    # 2. Upload to Gemini File API
    print(f"Uploading {filename} to Gemini File API...")
    try:
        sample_file = client.files.upload(file=file_path)
        print(f"File uploaded. ID: {sample_file.name}")
    except Exception as e:
        print(f"Error during upload: {e}")
        return

    # 3. Wait for processing
    print("Waiting for file to be processed...")
    while sample_file.state.name == "PROCESSING":
        time.sleep(2)
        sample_file = client.files.get(name=sample_file.name)

    if sample_file.state.name == "FAILED":
        print("Error: File processing failed on Gemini side.")
        return

    # 4. Initialize Model and Generate
    print(f"Running extraction using {MODEL_NAME}...")
    try:
        # Use the SYSTEM_PROMPT from get_single_file_prompt.py
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[sample_file, SYSTEM_PROMPT]
        )
    except Exception as e:
        print(f"Error during generation: {e}")
        return

    # 5. Save Result
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    
    print(f"\nExtraction successful!")
    print(f"Result saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/run_training_extraction.py <file_path>")
        sys.exit(1)

    file_path_arg = sys.argv[1]
    run_training_extraction(file_path_arg)
