# -*- coding: utf-8 -*-
"""
prepare_batch_input_vertex_json.py
----------------------------------
Generates a JSONL file for Vertex AI Gemini Batch processing using STRUCTURED OUTPUT.
Converts CSV few-shot examples to JSON and enforces a strict response schema.
"""

import os
import json
import pandas as pd
import io
import re
import csv
from get_single_file_prompt import get_file_prompt
from schemas import LaboratoryExtraction

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
TRAINING_DIR = os.path.join(PROJECT_ROOT, 'data', 'training_examples')
OUTPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_vertex_json_5.jsonl')

GCS_TRAINING_ROOT = "gs://fta-form26r-library/training_sets"
GCS_BATCH_ROOT = "gs://fta-form26r-library/full-set"

SYSTEM_PROMPT_JSON = """You are an expert environmental data chemist. Your task is to perform a surgical, comprehensive extraction of ALL analytical results from the attached laboratory reports.

**CRITICAL MANDATES (STRICT ENFORCEMENT):**
1.  **NO YAPPING in Data Fields:** Use the top-level 'reasoning' field ONLY for internal logic. ABSOLUTELY NO reasoning, internal thoughts, "Chain of Thought", or self-corrections are allowed in 'sample_notes', 'extraction_notes', or any other data field.
2.  **PRESERVE INEQUALITIES:** The 'result' field MUST include inequality signs (<, >) if they are present in the report (e.g., "< 0.05").
    *   **NO UNITS** in the 'result' field. Place them in 'units'.
    *   **NO ALPHABETIC QUALIFIERS** (U, J, B, etc.) in the 'result' field. Place them in 'qualifier_code'.
3.  **Verbatim Notes:** 'sample_notes' must ONLY contain verbatim text extracted from the report about sample condition (e.g., "Received on ice"). DO NOT add your own observations there.
4.  **Complete Metadata:** Extract 'lab_report_id', 'client_name', 'received_date', 'client_sample_id', and 'collection_date' for EVERY sample.

**Output Format:**
You MUST output valid JSON that matches the provided schema perfectly. Any text outside the 'reasoning' field that is not verbatim from the source will be considered a failure."""

CANONICAL_HEADERS = {
    "SAMPLES": [
        "lab_report_id", "lab_name", "client_name", "received_date", 
        "client_sample_id", "lab_sample_id", "collection_date", "matrix", 
        "sample_notes", "extraction_notes", "f26r_company_name", 
        "f26r_waste_location", "f26r_waste_code", "f26r_date_prepared"
    ],
    "RESULTS": [
        "lab_sample_id", "analyte", "result", "reporting_limit", "mdl", 
        "units", "qualifier_code", "dilution_factor", "analysis_date", 
        "method", "pdf_page_number"
    ],
    "QUALIFIERS": ["qualifier_code", "description"]
}

def resilient_parse_csv(csv_text, section_name):
    """Parses training CSVs line-by-line to avoid tokenization crashes."""
    lines = [l for l in csv_text.splitlines() if l.strip()]
    if not lines: return []
    
    canonical_cols = CANONICAL_HEADERS.get(section_name, [])
    
    # Check for header
    reader = csv.reader([lines[0]], quotechar='"', skipinitialspace=True)
    first_row = next(reader)
    start_idx = 0
    if any(term.lower() in [val.lower() for val in first_row] for term in canonical_cols):
        start_idx = 1
        
    records = []
    for line in lines[start_idx:]:
        try:
            reader = csv.reader([line], quotechar='"', skipinitialspace=True)
            row = next(reader)
            # Map to dict
            record = {}
            for i, col_name in enumerate(canonical_cols):
                record[col_name] = row[i] if i < len(row) else None
            records.append(record)
        except:
            pass
    return records

def parse_training_csv_to_json(csv_text):
    """Converts the custom ### SECTION CSV format to the JSON schema structure."""
    sections = {}
    current_section = None
    current_lines = []

    for line in csv_text.splitlines():
        if line.startswith("### "):
            if current_section:
                sections[current_section] = "\n".join(current_lines)
            current_section = line.replace("### ", "").strip()
            current_lines = []
        elif current_section:
            current_lines.append(line)
    if current_section:
        sections[current_section] = "\n".join(current_lines)

    result_obj = {
        "reasoning": "This is a clean training example showing surgical extraction from the provided PDF.",
        "samples": [], 
        "results": [], 
        "qualifiers": []
    }

    if "SAMPLES" in sections:
        result_obj["samples"] = resilient_parse_csv(sections["SAMPLES"], "SAMPLES")

    if "RESULTS" in sections:
        raw_results = resilient_parse_csv(sections["RESULTS"], "RESULTS")
        cleaned_results = []
        for r in raw_results:
            # ONLY strip alphabetic qualifiers, KEEP < and >
            if r.get("result") and r.get("qualifier_code"):
                q = str(r["qualifier_code"])
                res = str(r["result"])
                # Only strip if it's alphabetic
                if q and q.isalpha() and q in res:
                    r["result"] = res.replace(q, "").strip()
            cleaned_results.append(r)
        result_obj["results"] = cleaned_results

    if "QUALIFIERS" in sections:
        result_obj["qualifiers"] = resilient_parse_csv(sections["QUALIFIERS"], "QUALIFIERS")

    return json.dumps(result_obj)

def get_training_examples():
    examples = []
    files = [f for f in os.listdir(TRAINING_DIR) if f.startswith("training_") and f.endswith(".pdf")]
    
    for filename in sorted(files):
        pdf_gcs_uri = f"{GCS_TRAINING_ROOT}/{filename}"
        csv_filename = filename.replace("training_", "output_").replace(".pdf", ".csv")
        csv_path = os.path.join(TRAINING_DIR, csv_filename)

        if not os.path.exists(csv_path):
            continue

        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_text = f.read()

        json_text = parse_training_csv_to_json(csv_text)

        examples.append({
            "role": "user",
            "parts": [
                {"text": f"Extract results from this file: {filename}"},
                {"fileData": {"mimeType": "application/pdf", "fileUri": pdf_gcs_uri}}
            ]
        })
        examples.append({
            "role": "model",
            "parts": [{"text": json_text}]
        })
    return examples

def prepare_batch():
    print("Loading metadata...")
    triage = pd.read_parquet(TRIAGE_PATH)
    f26r = pd.read_parquet(F26R_PATH)

    triage_files = triage[['set_name', 'filename']].drop_duplicates()
    f26r_files = f26r[['set_name', 'filename']].drop_duplicates()
    merged = pd.merge(triage_files, f26r_files, on=['set_name', 'filename'])

    # Test on 10 files
    batch_files = merged.head(10)
    
    print("Building training turns...")
    training_turns = get_training_examples()

    # Get a clean schema for Vertex with strict descriptions to compel correct behavior
    response_schema = {
        "type": "object",
        "properties": {
            "reasoning": {"type": "string", "description": "Explain logic here. NO REASONING in any other field."},
            "samples": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "lab_report_id": {"type": "string", "description": "Main ID. NO REASONING."},
                        "lab_name": {"type": "string", "description": "Laboratory name. NO REASONING."},
                        "client_name": {"type": "string", "description": "Client name. NO REASONING."},
                        "received_date": {"type": "string", "description": "MM/DD/YY. NO REASONING."},
                        "client_sample_id": {"type": "string", "description": "Client sample ID. NO REASONING."},
                        "lab_sample_id": {"type": "string", "description": "Lab sample ID. NO REASONING."},
                        "collection_date": {"type": "string", "description": "MM/DD/YY HH:MM. NO REASONING."},
                        "matrix": {"type": "string", "description": "Matrix. NO REASONING."},
                        "sample_notes": {"type": "string", "description": "VERBATIM text from report about sample condition. ABSOLUTELY NO REASONING."},
                        "extraction_notes": {"type": "string", "description": "Technical notes only. ABSOLUTELY NO REASONING."},
                        "f26r_company_name": {"type": "string", "description": "From F26R Metadata. NO REASONING."},
                        "f26r_waste_location": {"type": "string", "description": "From F26R Metadata. NO REASONING."},
                        "f26r_waste_code": {"type": "string", "description": "From F26R Metadata. NO REASONING."},
                        "f26r_date_prepared": {"type": "string", "description": "From F26R Metadata. NO REASONING."}
                    }
                }
            },
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "lab_sample_id": {"type": "string", "description": "MUST match SAMPLES. NO REASONING."},
                        "analyte": {"type": "string", "description": "Chemical name only. NO REASONING."},
                        "result": {"type": "string", "description": "Reported value. INCLUDE signs (<, >) if present in report. NO units, NO alphabetic qualifiers. NO REASONING."},
                        "reporting_limit": {"type": "string", "description": "Numeric only. NO REASONING."},
                        "mdl": {"type": "string", "description": "Numeric only. NO REASONING."},
                        "units": {"type": "string", "description": "Unit only. NO REASONING."},
                        "qualifier_code": {"type": "string", "description": "Alphabetic flags only (U, J, B). Inequality signs should stay in the 'result' field. NO REASONING."},
                        "dilution_factor": {"type": "string", "description": "Numeric only. NO REASONING."},
                        "analysis_date": {"type": "string", "description": "Analysis date. NO REASONING."},
                        "method": {"type": "string", "description": "Method code. NO REASONING."},
                        "pdf_page_number": {"type": "string", "description": "Page number. NO REASONING."}
                    },
                    "required": ["lab_sample_id", "analyte"]
                }
            },
            "qualifiers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "qualifier_code": {"type": "string"},
                        "description": {"type": "string"}
                    }
                }
            }
        },
        "required": ["reasoning", "samples", "results"]
    }

    requests = []
    print(f"Preparing 10 JSON requests...")
    
    for idx, row in batch_files.iterrows():
        set_name = row['set_name']
        filename = row['filename']
        prompt = get_file_prompt(set_name, filename)
        
        # Strip CSV-specific rules from prompt to avoid confusing the model
        prompt_clean = prompt.replace("output as three distinct CSV sections", "output as JSON")
        
        file_gcs_uri = f"{GCS_BATCH_ROOT}/{set_name}/{filename}".replace("\\", "/")
        
        contents = list(training_turns)
        contents.append({
            "role": "user",
            "parts": [
                {"text": prompt_clean},
                {"fileData": {"mimeType": "application/pdf", "fileUri": file_gcs_uri}}
            ]
        })

        request_obj = {
            "id": file_gcs_uri,
            "request": {
                "contents": contents,
                "system_instruction": {"parts": [{"text": SYSTEM_PROMPT_JSON}]},
                "generation_config": {
                    "response_mime_type": "application/json",
                    "response_schema": response_schema
                }
            }
        }
        requests.append(request_obj)

    print(f"Writing 10 requests to {OUTPUT_JSONL}...")
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")

if __name__ == "__main__":
    prepare_batch()
