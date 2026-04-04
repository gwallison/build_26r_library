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

SYSTEM_PROMPT_JSON = """You are an expert environmental data chemist. Your task is to perform a comprehensive extraction of ALL analytical results from the attached laboratory reports.

**Extraction Scope:**
Extract every result including Inorganic, Organic, Metals, and Radiochemistry. Ignore Surrogates.

**Form 26R Association:**
For each lab sample, use the provided "Form 26R Metadata" list to find the most recent preceding 26R form in the document. Populate the f26r_ fields with that metadata.

**Output Format:**
You MUST output valid JSON that matches the provided schema perfectly."""

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
    expected_count = len(canonical_cols)
    
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

    result_obj = {"samples": [], "results": [], "qualifiers": []}

    if "SAMPLES" in sections:
        result_obj["samples"] = resilient_parse_csv(sections["SAMPLES"], "SAMPLES")

    if "RESULTS" in sections:
        result_obj["results"] = resilient_parse_csv(sections["RESULTS"], "RESULTS")

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

    # Test on 5 files
    batch_files = merged.head(5)
    
    print("Building training turns...")
    training_turns = get_training_examples()

    # Get a clean schema for Vertex (stripping $defs and $ref which can break the importer)
    response_schema = {
        "type": "object",
        "properties": {
            "samples": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "lab_report_id": {"type": "string"},
                        "lab_name": {"type": "string"},
                        "client_name": {"type": "string"},
                        "received_date": {"type": "string"},
                        "client_sample_id": {"type": "string"},
                        "lab_sample_id": {"type": "string"},
                        "collection_date": {"type": "string"},
                        "matrix": {"type": "string"},
                        "sample_notes": {"type": "string"},
                        "extraction_notes": {"type": "string"},
                        "f26r_company_name": {"type": "string"},
                        "f26r_waste_location": {"type": "string"},
                        "f26r_waste_code": {"type": "string"},
                        "f26r_date_prepared": {"type": "string"}
                    }
                }
            },
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "lab_sample_id": {"type": "string"},
                        "analyte": {"type": "string"},
                        "result": {"type": "string"},
                        "reporting_limit": {"type": "string"},
                        "mdl": {"type": "string"},
                        "units": {"type": "string"},
                        "qualifier_code": {"type": "string"},
                        "dilution_factor": {"type": "string"},
                        "analysis_date": {"type": "string"},
                        "method": {"type": "string"},
                        "pdf_page_number": {"type": "string"}
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
        "required": ["samples", "results"]
    }

    requests = []
    print(f"Preparing 5 JSON requests...")
    
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

    print(f"Writing 5 requests to {OUTPUT_JSONL}...")
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req) + '\n')
            
    print("Done!")

if __name__ == "__main__":
    prepare_batch()
