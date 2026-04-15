# -*- coding: utf-8 -*-
"""
test_single_file_surgical.py
----------------------------
Methodical test of the "Surgical" extraction schema on a single PDF.
Uses direct API call for immediate feedback.
"""

import os
import json
from google import genai
from google.genai import types
from schemas_surgical import SurgicalExtraction

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ID = "open-ff-catalog-1"
LOCATION = "us-central1"
MODEL_NAME = "gemini-2.5-flash" # Use 2.5 flash which was in the list

# Test File Context
SET_NAME = "2021-2022"
FILENAME = "003297_Edsell Comp_470 Inlet 2021.pdf"
GCS_URI = f"gs://fta-form26r-library/full-set/{SET_NAME}/{FILENAME}"

# Metadata from Triage/F26R
FLAGGED_PAGES = [10, 14, 15] # Known results pages for testing density
F26R_META = {
    "f_co": "Repsol Oil & Gas USA, LLC",
    "f_loc": "Filters are generated during normal operations and maintenance at the Edsell Compressor Station...",
    "f_code": "NaN",
    "f_dt": "2/28/2022"
}

SYSTEM_PROMPT = f"""You are an expert environmental data chemist. Your task is to perform a surgical, comprehensive extraction of ALL analytical results from the attached laboratory reports.

**CRITICAL MANDATES:**
1. **Output ONLY the JSON object.** No yapping. No reasoning. 
2. **Short Keys:** Use the single/double letter keys defined in the schema (sid, cid, a, r, rl, mdl, u, q, p).
3. **Preserve Inequalities:** Keep < and > in the 'r' field.
4. **No Units/Qualifiers in 'r':** Put units in 'u' and flags (U, J, B) in 'q'.
5. **Consolidate Meta:** Use the provided F26R metadata for the 'meta' block.
6. **IGNORE QC:** Skip any results labeled as "Method Blank", "LCS", "Surrogate", or found under a "Quality Control Data" header.

**F26R Context for this file:**
{F26R_META}

**Target Pages:**
Extract data from the following pages of the PDF: {FLAGGED_PAGES}
"""

def run_test():
    # Use Vertex AI mode for GCS support
    client = genai.Client(
        vertexai=True,
        project=PROJECT_ID,
        location=LOCATION
    )

    print(f"Starting SURGICAL extraction test (VERTEX AI MODE) for: {FILENAME}")
    print(f"Processing {len(FLAGGED_PAGES)} flagged pages...")

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[
                types.Part.from_uri(file_uri=GCS_URI, mime_type="application/pdf"),
                types.Part.from_text(text="Perform the surgical extraction as per instructions.")
            ],
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                response_mime_type="application/json",
                response_schema=SurgicalExtraction.model_json_schema()
            )
        )

        # Parse and Validate
        text = response.text
        print("\n--- RAW RESPONSE ---")
        print(text)
        
        # Save output
        output_file = "data/output/test_surgical_result.json"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(text)
        
        # Validate with Pydantic
        data = json.loads(text)
        validated = SurgicalExtraction(**data)
        
        print(f"\nSUCCESS!")
        print(f"Extracted Samples: {len(validated.samples)}")
        print(f"Extracted Results: {len(validated.results)}")
        print(f"Saved to: {output_file}")

    except Exception as e:
        print(f"\nFAILED: {e}")

if __name__ == "__main__":
    run_test()
