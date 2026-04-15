# -*- coding: utf-8 -*-
"""
prepare_batch_input_surgical_v2.py
----------------------------------
Generates a JSONL file for Vertex AI Gemini Batch processing using LEAN SURGICAL output.
Points directly to the physically split 'Micro-PDFs' uploaded to GCS.
Metadata association is handled post-harvest via chunk_map.parquet.
"""

import os
import json
import sys
import pandas as pd
import random

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_surgical_v2.jsonl')
OUTPUT_JSONL_SAMPLE = os.path.join(PROJECT_ROOT, 'data', 'batch_input_surgical_v2_sample.jsonl')
CHUNKED_PDFS_DIR = os.path.join(PROJECT_ROOT, 'data', 'chunked_pdfs')

GCS_CHUNKED_ROOT = "gs://fta-form26r-library/chunked-pdfs-v2"

SYSTEM_PROMPT = """You are an expert environmental data chemist. Your task is to perform a surgical, comprehensive extraction of ALL analytical results from the attached laboratory reports.

**CRITICAL MANDATES:**
1. **Output ONLY the JSON object.** No yapping. No reasoning. 
2. **Short Keys:** Use the single/double letter keys defined in the schema (sid, cid, a, r, rl, mdl, u, q, p).
3. **Preserve Inequalities:** Keep < and > in the 'r' field.
4. **No Units/Qualifiers in 'r':** Put units in 'u' and flags (U, J, B) in 'q'.
5. **IGNORE QC:** Skip any results labeled as "Method Blank", "LCS", "Surrogate", or found under a "Quality Control Data" header.
6. **DATE FORMAT:** Extract dates as MM/DD/YYYY. Do NOT include timezone strings or repeat timezone offsets.
"""

# Define a FLAT schema (Lean: No 'meta' block)
FLAT_SCHEMA_LEAN = {
    "type": "object",
    "properties": {
        "samples": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sid": {"type": "string", "description": "Lab Sample ID"},
                    "cid": {"type": "string", "description": "Client Sample ID"},
                    "rd": {"type": "string", "description": "Received Date"},
                    "cd": {"type": "string", "description": "Collection Date"},
                    "m": {"type": "string", "description": "Matrix"},
                    "bad": {"type": "boolean", "description": "True if scan is poor/blurry"}
                },
                "required": ["sid"]
            }
        },
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "sid": {"type": "string", "description": "Must match sid in samples"},
                    "a": {"type": "string", "description": "Analyte name"},
                    "r": {"type": "string", "description": "Value (include < or >)"},
                    "rl": {"type": "string", "description": "Reporting Limit"},
                    "mdl": {"type": "string", "description": "MDL"},
                    "u": {"type": "string", "description": "Units"},
                    "q": {"type": "string", "description": "Qualifiers (U, J, B)"},
                    "p": {"type": "string", "description": "Page Number within this PDF"}
                },
                "required": ["sid", "a", "p"]
            }
        }
    },
    "required": ["samples", "results"]
}

def prepare_batch(sample_fraction=None, skip_processed=True):
    # Build requests based on the micro-PDFs present in the chunked directory
    chunk_map_path = os.path.join(PROJECT_ROOT, 'data', 'output', 'chunk_map.parquet')
    if not os.path.exists(chunk_map_path):
        print(f"Error: {chunk_map_path} not found.")
        return
    
    chunk_map = pd.read_parquet(chunk_map_path)
    
    if skip_processed:
        tracker_path = os.path.join(PROJECT_ROOT, 'data', 'output', 'processed_files.parquet')
        if os.path.exists(tracker_path):
            tracker = pd.read_parquet(tracker_path)
            processed_originals = tracker['filename'].unique().tolist()
            # Only include chunks whose original_filename is NOT in the processed list
            unprocessed_chunks = chunk_map[~chunk_map['original_filename'].isin(processed_originals)]
            chunked_files = unprocessed_chunks['chunk_filename'].unique().tolist()
            print(f"Skipping {len(chunk_map['chunk_filename'].unique()) - len(chunked_files)} already processed chunks.")
        else:
            chunked_files = chunk_map['chunk_filename'].unique().tolist()
    else:
        chunked_files = chunk_map['chunk_filename'].unique().tolist()

    if sample_fraction:
        sample_size = int(len(chunked_files) * sample_fraction)
        print(f"Sampling {sample_size} files ({sample_fraction*100}% of {len(chunked_files)} remaining)...")
        chunked_files = random.sample(chunked_files, sample_size)
        target_output = OUTPUT_JSONL_SAMPLE
    else:
        target_output = OUTPUT_JSONL

    requests = []
    print(f"Preparing lean surgical requests for {len(chunked_files)} micro-PDFs...")

    for chunk_file in chunked_files:
        file_gcs_uri = f"{GCS_CHUNKED_ROOT}/{chunk_file}"
        
        # ID is just the chunk filename for mapping back later
        request_obj = {
            "id": chunk_file, 
            "request": {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": "Extract all results from this file."},
                            {"fileData": {"mimeType": "application/pdf", "fileUri": file_gcs_uri}}
                        ]
                    }
                ],
                "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
                "generation_config": {
                    "response_mime_type": "application/json",
                    "response_schema": FLAT_SCHEMA_LEAN
                }
            }
        }
        requests.append(request_obj)

    print(f"Total requests created: {len(requests)}")
    print(f"Writing requests to {target_output}...")
    
    with open(target_output, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")
    return target_output

if __name__ == "__main__":
    is_sample = "--sample" in sys.argv
    # For this 20% test, we use 0.20
    prepare_batch(sample_fraction=0.20 if is_sample else None)
