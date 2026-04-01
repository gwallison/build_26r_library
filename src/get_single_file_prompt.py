# -*- coding: utf-8 -*-
"""
get_single_file_prompt.py
-------------------------
Generates a targeted Gemini prompt for a single PDF file using 
26R and Lab Report triage metadata. Output is structured as 
three distinct CSV tables.
"""

import os
import sys
import pandas as pd
import json

# ---------------------------------------------------------------------------
# Paths & Template
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')

SYSTEM_PROMPT = """You are an expert environmental data chemist and data extraction specialist. Your task is to perform a comprehensive extraction of ALL analytical results contained within the attached laboratory reports. 

**Extraction Scope:**
Do not filter by analyte type. Extract every result listed in the "Analytical Results," "Sample Summary," or "Certificate of Analysis" sections, including Inorganic Chemistry, Organic Chemistry (VOCs, SVOCs), Metals, and Radiochemistry.

Ignore Quality Control (QC) Summary sheets and Initial Calibration Verifications (ICV) unless specifically requested.

**Extraction Rules:**

1. **Relational Structure:** Provide the output as three distinct CSV sections (SAMPLES, RESULTS, and QUALIFIERS). 
2. **CSV Formatting:** Use a standard CSV format. **Crucially, any field containing a comma (especially chemical names or notes) MUST be enclosed in double quotes (e.g., "1,2-Dichloroethane").**
3. **Grounding/Citations:** Do NOT include any citations like "[cite: X]" or grounding markers in the CSV text. The output must be pure CSV data.
4. **Radiological Specifics:** For radiological rows, combine the Result and the Uncertainty (e.g., "15.2 +/- 1.4") into the `result` field.
5. **Notes/Comments:** Capture any sample-specific comments or notes in the `sample_notes` field of the SAMPLES table.
6. **Form 26R Association:** For each lab sample identified, use the provided "Form 26R Metadata" list to find the **most recent preceding** 26R form in the document. Populate the `f26r_` fields in the SAMPLES table with the metadata from that specific form. If no 26R form precedes the lab report, leave these fields empty.

---

**Output Format:**
Return three distinct CSV blocks, each preceded by a header (e.g., "### SAMPLES").

### SAMPLES
Columns:
- lab_report_id: The Lab Report or Job ID.
- lab_name: The name of the laboratory.
- received_date: The date the laboratory received the samples.
- client_sample_id: The Client's unique ID for the sample.
- lab_sample_id: The Laboratory's unique ID for the sample (Primary Key).
- collection_date: The date/time the sample was collected.
- matrix: The sample matrix (e.g., Water, Soil).
- sample_notes: Any specific comments, hold-time warnings, or observations for this sample.
- f26r_company_name: The Company Name from the most recent preceding Form 26R.
- f26r_waste_location: The Waste Location from the most recent preceding Form 26R.
- f26r_waste_code: The Waste Code from the most recent preceding Form 26R.
- f26r_date_prepared: The Date Prepared from the most recent preceding Form 26R.

### RESULTS
Columns:
- lab_sample_id: The Laboratory's unique ID for the sample (Foreign Key).
- analyte: The full name of the parameter.
- result: The numerical value, "ND", or radiological result (e.g., "15.2 +/- 1.4").
- reporting_limit: The limit of quantitation (RL/PQL).
- mdl: The method detection limit (if provided).
- units: (e.g., mg/L, ug/kg, pCi/L).
- qualifier_code: The flag found in the results (e.g., J, U, B).
- dilution_factor: The numerical dilution applied.
- analysis_date: The date the specific test was run.
- method: The specific analytical method used (e.g., EPA 6010D).

### QUALIFIERS
Columns:
- qualifier_code: The flag (e.g., J, U).
- description: The definition of the qualifier code as found in the report's glossary or footnotes.
"""

def get_file_prompt(set_name, filename):
    if not os.path.exists(TRIAGE_PATH) or not os.path.exists(F26R_PATH):
        print("Error: Metadata files not found.")
        return

    df_triage = pd.read_parquet(TRIAGE_PATH)
    df_26r = pd.read_parquet(F26R_PATH)

    # Filter for the specific file's triage results
    lab_pages = sorted(df_triage[(df_triage['set_name'] == set_name) & 
                                (df_triage['filename'] == filename)]['page_number'].tolist())
    
    # Filter for the specific file's 26R metadata
    file_26r = df_26r[(df_26r['set_name'] == set_name) & (df_26r['filename'] == filename)].copy()
    
    # We want unique forms (unique page numbers)
    file_26r = file_26r.drop_duplicates(subset=['page_number']).sort_values('page_number')

    # Build the guide section
    guide = f"\n\n**File-Specific Context for {filename}:**\n"
    
    if not file_26r.empty:
        guide += "### Form 26R Metadata (for association guidance):\n"
        for _, row in file_26r.iterrows():
            guide += f"- Page {row.page_number}: Company='{row.company_name}', Location='{row.waste_location}', Code='{row.waste_code}', Date='{row.date_prepared}'\n"
        guide += "*(Do not extract results from these 26R pages; use them only to populate the f26r_ metadata fields for subsequent lab reports.)*\n"
    else:
        guide += "- No Form 26R pages identified in this file.\n"
        
    if lab_pages:
        guide += f"\n### Target Lab Report Pages:\n- {', '.join(map(str, lab_pages))}\n"
    else:
        guide += "\n- No specific lab report pages flagged by triage; please scan the entire document for analytical results.\n"

    return SYSTEM_PROMPT + guide

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python src/get_single_file_prompt.py <set_name> <filename>")
        sys.exit(1)

    set_name_arg = sys.argv[1]
    filename_arg = sys.argv[2]

    prompt = get_file_prompt(set_name_arg, filename_arg)
    if prompt:
        print(prompt)
