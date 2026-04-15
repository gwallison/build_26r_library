# -*- coding: utf-8 -*-
"""
build_context_cache.py
-----------------------
Uploads 16 Gold Standard training PDF/CSV pairs and creates a 
Gemini Context Cache to reduce costs and enable faster extraction.

Returns: CACHE_ID
"""

import os
import time
from google import genai
from google.genai import types
from get_single_file_prompt import SYSTEM_PROMPT

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_KEY = os.getenv("GoogleAI-API-key")
# Use gemini-3-flash-preview as requested
MODEL_NAME = "gemini-3-flash-preview" 
CACHE_TTL_MINUTES = 1440

if not API_KEY:
    print("Error: Environment variable 'GoogleAI-API-key' not set.")
    import sys
    sys.exit(1)

client = genai.Client(api_key=API_KEY)

def upload_and_wait(file_path):
    """Uploads a file and waits for processing."""
    print(f"Uploading {os.path.basename(file_path)}...")
    file_ref = client.files.upload(file=file_path)
    
    while file_ref.state.name == "PROCESSING":
        time.sleep(2)
        file_ref = client.files.get(name=file_ref.name)
        
    if file_ref.state.name == "FAILED":
        raise Exception(f"File processing failed for {file_path}")
    
    return file_ref

def build_cache():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    training_dir = os.path.join(project_root, 'data', 'training_examples')

    # 1. Gather all training files
    files = [f for f in os.listdir(training_dir) if f.startswith("training_") and f.endswith(".pdf")]
    
    # 2. Build the Content parts for the cache
    # In Gemini context caching, the "contents" are the few-shot examples.
    # We will build them as a list of parts.
    cache_contents = []

    for filename in sorted(files):
        pdf_path = os.path.join(training_dir, filename)
        csv_filename = filename.replace("training_", "output_").replace(".pdf", ".csv")
        csv_path = os.path.join(training_dir, csv_filename)

        if not os.path.exists(csv_path):
            print(f"Warning: CSV match not found for {filename}. Skipping.")
            continue

        # Upload PDF
        pdf_ref = upload_and_wait(pdf_path)
        
        # Read Corrected CSV
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_text = f.read()

        # Build the user/model turn pair for this example
        # User side (the PDF)
        cache_contents.append(
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=f"Extract results from this file: {filename}"),
                    types.Part.from_uri(file_uri=pdf_ref.uri, mime_type="application/pdf")
                ]
            )
        )
        # Model side (the correct CSV)
        cache_contents.append(
            types.Content(
                role="model",
                parts=[types.Part.from_text(text=csv_text)]
            )
        )

    # 3. Create the Context Cache
    print("\nCreating Context Cache...")
    
    try:
        # Note: 'system_instruction' must be a single Content object
        # 'contents' is the list of example messages
        cache = client.caches.create(
            model=MODEL_NAME,
            config=types.CreateCachedContentConfig(
                display_name="extraction-training-cache",
                system_instruction=types.Content(
                    parts=[types.Part.from_text(text=SYSTEM_PROMPT)]
                ),
                contents=cache_contents,
                ttl=f"{CACHE_TTL_MINUTES * 60}s"
            )
        )
        print(f"\nCache Created Successfully!")
        print(f"Cache Name: {cache.name}")
        print(f"Model: {cache.model}")
        print(f"Expires: {cache.expire_time}")
        return cache.name
    except Exception as e:
        print(f"Error creating cache: {e}")
        return None

if __name__ == "__main__":
    build_cache()
