# -*- coding: utf-8 -*-
"""
run_single_file_extraction.py
-----------------------------
Uploads a single PDF to Gemini File API and runs the extraction prompt.
Saves the result to data/output/single_test/

Usage:
    python src/run_single_file_extraction.py <set_name> <filename>
"""

import os
import sys
import time
from google import genai
from get_single_file_prompt import get_file_prompt

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
MODEL_NAME = "gemini-3-flash-preview"
OUTPUT_DIR = os.path.join("data", "output", "single_test")
PDF_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

# Initialize Client
client = genai.Client(api_key=API_KEY)

def run_extraction(set_name, filename):
    # 1. Get the prompt
    prompt = get_file_prompt(set_name, filename)
    if not prompt:
        return

    # 2. Locate the file
    file_path = os.path.join(PDF_ROOT, set_name, filename)
    if not os.path.exists(file_path):
        print(f"Error: PDF not found at {file_path}")
        return

    # 3. Upload to Gemini File API
    print(f"Uploading {filename} to Gemini File API...")
    try:
        # The new SDK uses 'file' for the path
        sample_file = client.files.upload(file=file_path)
        print(f"File uploaded. ID: {sample_file.name}")
    except Exception as e:
        print(f"Error during upload: {e}")
        return

    # 4. Wait for processing
    print("Waiting for file to be processed...")
    while sample_file.state.name == "PROCESSING":
        time.sleep(2)
        sample_file = client.files.get(name=sample_file.name)

    if sample_file.state.name == "FAILED":
        print("Error: File processing failed on Gemini side.")
        return

    # 5. Initialize Model and Generate
    print(f"Running extraction using {MODEL_NAME}...")
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[sample_file, prompt]
        )
    except Exception as e:
        print(f"Error during generation: {e}")
        return

    # 6. Save Result
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name = filename.replace(" ", "_").replace(".pdf", "")
    output_path = os.path.join(OUTPUT_DIR, f"{safe_name}_results.txt")
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    
    print(f"\nExtraction successful!")
    print(f"Result saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python src/run_single_file_extraction.py <set_name> <filename>")
        sys.exit(1)

    set_name_arg = sys.argv[1]
    filename_arg = sys.argv[2]

    run_extraction(set_name_arg, filename_arg)
