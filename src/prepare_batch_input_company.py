# -*- coding: utf-8 -*-
"""
prepare_batch_input_company.py
------------------------------
Generates a JSONL file for Vertex AI Gemini Batch processing to extract 
company/client names from the first page of files that lack Form 26Rs.
"""

import os
import json
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
CHUNK_MAP_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'chunk_map.parquet')
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_company.jsonl')

GCS_COMPANY_ROOT = "gs://fta-form26r-library/company-extraction-pages"

SYSTEM_PROMPT = """You are an expert at administrative document analysis. 
Identify the 'Client' (the company that ordered the testing) and the 'Laboratory' (the company performing the analysis) from this page.
The 'Client' is often listed after "Prepared For:", "Client:", "To:", or in the top-left header of a cover letter.
The 'Laboratory' is usually the main entity in the large header/logo of the report.

**CRITICAL MANDATES:**
1. Output ONLY the JSON object.
2. 'c' = Client Name.
3. 'l' = Lab Name.
4. 'conf' = HIGH if you are very sure, LOW if it is a best guess, NONE if not found.
"""

COMPANY_SCHEMA = {
    "type": "object",
    "properties": {
        "c": {"type": "string", "description": "Client/Ordering Company Name"},
        "l": {"type": "string", "description": "Analytical Lab Name"},
        "conf": {"type": "string", "description": "Confidence: HIGH, LOW, or NONE"}
    },
    "required": ["conf"]
}

def prepare():
    print("Loading dataframes...")
    corpus = pd.read_parquet(CORPUS_PATH, columns=['filename'])
    f26r = pd.read_parquet(F26R_PATH, columns=['filename'])

    # 1. Identify files without F26Rs
    files_with_f26r = set(f26r['filename'].unique())
    all_files = set(corpus['filename'].unique())
    target_files = sorted(list(all_files - files_with_f26r))
    
    print(f"Total files in corpus: {len(all_files)}")
    print(f"Files with F26Rs: {len(files_with_f26r)}")
    print(f"Target files without F26Rs: {len(target_files)}")

    requests = []
    for filename in target_files:
        # Construct the safe name used by the splitter
        base_name = os.path.splitext(filename)[0]
        safe_name = base_name.replace(" ", "_").replace(".", "_")
        gcs_uri = f"{GCS_COMPANY_ROOT}/{safe_name}_page1.pdf"
        
        request_obj = {
            "id": filename, 
            "request": {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": "Extract company names from the first page of this document."},
                            {"fileData": {"mimeType": "application/pdf", "fileUri": gcs_uri}}
                        ]
                    }
                ],
                "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
                "generation_config": {
                    "response_mime_type": "application/json",
                    "response_schema": COMPANY_SCHEMA
                }
            }
        }
        requests.append(request_obj)

    print(f"Prepared {len(requests)} requests pointing to {GCS_COMPANY_ROOT}")

    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print(f"Input written to {OUTPUT_JSONL}")

if __name__ == "__main__":
    prepare()
