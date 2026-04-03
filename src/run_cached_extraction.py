# -*- coding: utf-8 -*-
"""
run_cached_extraction.py
-------------------------
Runs extraction on a library PDF file using a pre-built Context Cache.
Integrates triage metadata (target pages and 26R context).

Usage:
    python src/run_cached_extraction.py <cache_id> <set_name> <filename>
"""

import os
import sys
import time
from google import genai
from google.genai import types
from get_single_file_prompt import get_file_prompt, SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_ROOT = os.path.join(PROJECT_ROOT, "data", "corpus")

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def run_extraction(cache_id, set_name, filename):
    # 1. Resolve Path to Library File
    if set_name == "training":
        # Handle training files if requested
        pdf_path = os.path.join(PROJECT_ROOT, "data", "training_examples", filename)
    else:
        pdf_path = os.path.join(CORPUS_ROOT, set_name, filename)

    if not os.path.exists(pdf_path):
        print(f"Error: File not found at {pdf_path}")
        return

    # 2. Get File-Specific Context (Target Pages and 26R Metadata)
    print(f"Fetching metadata for {filename} in set {set_name}...")
    full_prompt = get_file_prompt(set_name, filename)
    if not full_prompt:
        print("Error: Could not generate prompt metadata.")
        return
    
    # Extract just the "File-Specific Context" part
    file_guide = full_prompt.replace(SYSTEM_PROMPT, "").strip()

    # 3. Upload Target File
    print(f"Uploading target file: {filename}...")
    sample_file = client.files.upload(file=pdf_path)
    
    while sample_file.state.name == "PROCESSING":
        time.sleep(2)
        sample_file = client.files.get(name=sample_file.name)

    if sample_file.state.name == "FAILED":
        print("Error: File processing failed.")
        return

    # 4. Generate Content using the Cache + File-Specific Guide
    print(f"Running extraction using Cache: {cache_id}...")
    try:
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=[
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text=file_guide),
                        types.Part.from_uri(file_uri=sample_file.uri, mime_type="application/pdf")
                    ]
                )
            ],
            config=types.GenerateContentConfig(
                cached_content=cache_id
            )
        )
    except Exception as e:
        print(f"Error during generation: {e}")
        return

    # 5. Save Result
    out_dir = os.path.join(PROJECT_ROOT, "data", "output_cached")
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    
    # Format: output_cached_<set_name>_<filename>.csv
    out_filename = f"output_cached_{set_name}_{os.path.splitext(filename)[0]}.csv"
    out_path = os.path.join(out_dir, out_filename)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    
    print(f"\nExtraction successful!")
    print(f"Result saved to: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python src/run_cached_extraction.py <cache_id> <set_name> <filename>")
        sys.exit(1)

    cid = sys.argv[1]
    sname = sys.argv[2]
    fname = sys.argv[3]
    run_extraction(cid, sname, fname)
