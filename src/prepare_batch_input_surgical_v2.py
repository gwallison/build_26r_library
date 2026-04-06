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
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_surgical_v2.jsonl')
CHUNKED_PDFS_DIR = os.path.join(PROJECT_ROOT, 'data', 'chunked_pdfs')

GCS_CHUNKED_ROOT = "gs://fta-form26r-library/chunked-pdfs"

SYSTEM_PROMPT = """You are an expert environmental data chemist. Your task is to perform a surgical, comprehensive extraction of ALL analytical results from the attached laboratory reports.

**CRITICAL MANDATES:**
1. **Output ONLY the JSON object.** No yapping. No reasoning. 
2. **Short Keys:** Use the single/double letter keys defined in the schema (sid, cid, a, r, rl, mdl, u, q, p).
3. **Preserve Inequalities:** Keep < and > in the 'r' field.
4. **No Units/Qualifiers in 'r':** Put units in 'u' and flags (U, J, B) in 'q'.
5. **IGNORE QC:** Skip any results labeled as "Method Blank", "LCS", "Surrogate", or found under a "Quality Control Data" header.
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

def prepare_batch():
    # Build requests based on the micro-PDFs present in the chunked directory
    chunked_files = [f for f in os.listdir(CHUNKED_PDFS_DIR) if f.endswith(".pdf")]
    
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
    print(f"Writing requests to {OUTPUT_JSONL}...")
    
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")

if __name__ == "__main__":
    prepare_batch()
