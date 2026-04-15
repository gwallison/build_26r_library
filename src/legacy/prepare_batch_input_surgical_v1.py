# -*- coding: utf-8 -*-
"""
prepare_batch_input_surgical_v1.py
----------------------------------
Generates a JSONL file for Vertex AI Gemini Batch processing using SURGICAL output.
Chunks large PDFs into groups of 15 flagged pages to avoid 8192 token limit.
"""

import os
import json
import pandas as pd
from schemas_surgical import SurgicalExtraction

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_surgical_v1.jsonl')

GCS_BATCH_ROOT = "gs://fta-form26r-library/full-set"
CHUNK_SIZE = 15

SYSTEM_PROMPT = """You are an expert environmental data chemist. Your task is to perform a surgical, comprehensive extraction of ALL analytical results from the attached laboratory reports.

**CRITICAL MANDATES:**
1. **Output ONLY the JSON object.** No yapping. No reasoning. 
2. **Short Keys:** Use the single/double letter keys defined in the schema (sid, cid, a, r, rl, mdl, u, q, p).
3. **Preserve Inequalities:** Keep < and > in the 'r' field.
4. **No Units/Qualifiers in 'r':** Put units in 'u' and flags (U, J, B) in 'q'.
5. **IGNORE QC:** Skip any results labeled as "Method Blank", "LCS", "Surrogate", or found under a "Quality Control Data" header.
"""

def prepare_batch():
    print("Loading metadata...")
    triage = pd.read_parquet(TRIAGE_PATH)
    f26r = pd.read_parquet(F26R_PATH)

    # Filter to our 10-file torture test set for this run
    # (Actually, let's just do all files that have triage signal to be more methodical)
    # But for THIS first test, I'll limit to the 10 files we discussed.
    
    triage_files = triage[['set_name', 'filename']].drop_duplicates()
    f26r_files = f26r[['set_name', 'filename']].drop_duplicates()
    merged = pd.merge(triage_files, f26r_files, on=['set_name', 'filename'])
    batch_files = merged.head(10)

    requests = []
    print(f"Preparing surgical requests for {len(batch_files)} files...")

    # Define a FLAT schema to avoid $ref/$defs which Vertex AI Batch fails on
    flat_schema = {
        "type": "object",
        "properties": {
            "meta": {
                "type": "object",
                "properties": {
                    "rid": {"type": "string", "description": "Lab Report ID"},
                    "ln": {"type": "string", "description": "Lab Name"},
                    "cn": {"type": "string", "description": "Client Name"},
                    "f_co": {"type": "string", "description": "F26R Company"},
                    "f_loc": {"type": "string", "description": "F26R Location"},
                    "f_code": {"type": "string", "description": "F26R Code"},
                    "f_dt": {"type": "string", "description": "F26R Date"}
                }
            },
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
                        "p": {"type": "string", "description": "PDF Page Number"}
                    },
                    "required": ["sid", "a"]
                }
            }
        },
        "required": ["meta", "samples", "results"]
    }

    for _, row in batch_files.iterrows():
        sn = row['set_name']
        fn = row['filename']
        
        # Get flagged pages for this file
        file_pages = sorted(triage[(triage['set_name']==sn) & (triage['filename']==fn)]['page_number'].tolist())
        
        # Get F26R metadata (take first prepared form as representative for 'meta' block)
        meta_row = f26r[(f26r['set_name']==sn) & (f26r['filename']==fn)].iloc[0]
        f26r_context = {
            "f_co": str(meta_row.get('company_name', '')),
            "f_loc": str(meta_row.get('waste_location', '')),
            "f_code": str(meta_row.get('waste_code', '')),
            "f_dt": str(meta_row.get('date_prepared', ''))
        }

        # Chunk the pages
        for i in range(0, len(file_pages), CHUNK_SIZE):
            chunk = file_pages[i : i + CHUNK_SIZE]
            
            file_gcs_uri = f"{GCS_BATCH_ROOT}/{sn}/{fn}".replace("\\", "/")
            
            user_text = f"Extract results from the following pages of the PDF: {chunk}\n\nUse this F26R Metadata for the 'meta' block:\n{json.dumps(f26r_context)}"
            
            # Note: We are using a flat dict schema instead of SurgicalExtraction.model_json_schema()
            request_obj = {
                "id": f"{file_gcs_uri}_chunk_{i//CHUNK_SIZE}",
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {"text": user_text},
                                {"fileData": {"mimeType": "application/pdf", "fileUri": file_gcs_uri}}
                            ]
                        }
                    ],
                    "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
                    "generation_config": {
                        "response_mime_type": "application/json",
                        "response_schema": flat_schema
                    }
                }
            }
            requests.append(request_obj)

    print(f"Total chunks created: {len(requests)}")
    print(f"Writing requests to {OUTPUT_JSONL}...")
    
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")

if __name__ == "__main__":
    prepare_batch()
