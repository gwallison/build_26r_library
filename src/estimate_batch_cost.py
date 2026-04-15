import os
import json
import fitz  # PyMuPDF
from tqdm import tqdm

# Pricing for US-CENTRAL1 Batch (50% of standard online pricing)
# Updated for 2026: Gemini 3.1 Flash-Lite is the new budget standard
PRICING = {
    "flash-lite": {
        "input": 0.125 / 1_000_000,
        "output": 0.75 / 1_000_000
    },
    "flash": {
        "input": 0.075 / 1_000_000,
        "output": 0.30 / 1_000_000
    },
    "pro": {
        "input": 0.625 / 1_000_000,
        "output": 5.00 / 1_000_000
    }
}

TOKENS_PER_PAGE = 258

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Default local JSONL path
DEFAULT_INPUT_JSONL = os.path.join(PROJECT_ROOT, 'data', 'batch_input_surgical_v2.jsonl')
CHUNKED_PDFS_DIR = os.path.join(PROJECT_ROOT, 'data', 'chunked_pdfs')

def estimate_cost(jsonl_path=DEFAULT_INPUT_JSONL):
    if not os.path.exists(jsonl_path):
        return None

    total_input_tokens = 0
    total_pages = 0
    request_count = 0
    
    # We'll estimate output tokens differently for Lite vs Full
    # Lite should have much fewer thought tokens if configured correctly.
    # Full (2.5+) has massive CoT overhead.
    EST_OUTPUT_TOKENS_PER_CHUNK_FULL = 10000 
    EST_OUTPUT_TOKENS_PER_CHUNK_LITE = 1500 

    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading JSONL: {e}")
        return None

    request_count = len(lines)

    # Cache for page counts to avoid re-reading same PDF if repeated
    page_count_cache = {}

    for line in tqdm(lines, desc="Estimating tokens", leave=False):
        try:
            data = json.loads(line)
        except:
            continue
            
        request = data.get("request", {})
        
        # 1. Count text tokens (rough estimate: chars / 4)
        system_text = ""
        sys_inst = request.get("system_instruction", {})
        if sys_inst:
            parts = sys_inst.get("parts", [])
            if parts:
                system_text = parts[0].get("text", "")
                
        user_text = ""
        contents = request.get("contents", [])
        if contents:
            parts = contents[0].get("parts", [])
            for part in parts:
                if "text" in part:
                    user_text += part["text"]
        
        # Add schema tokens
        schema_text = json.dumps(request.get("generation_config", {}).get("response_schema", {}))
        
        prompt_tokens = (len(system_text) + len(user_text) + len(schema_text)) // 4
        total_input_tokens += prompt_tokens
        
        # 2. Count PDF tokens
        pdf_filename = data.get("id")
        if pdf_filename:
            if pdf_filename not in page_count_cache:
                pdf_path = os.path.join(CHUNKED_PDFS_DIR, pdf_filename)
                if os.path.exists(pdf_path):
                    try:
                        doc = fitz.open(pdf_path)
                        page_count_cache[pdf_filename] = doc.page_count
                        doc.close()
                    except:
                        page_count_cache[pdf_filename] = 0
                else:
                    page_count_cache[pdf_filename] = 0
            
            pages = page_count_cache[pdf_filename]
            total_pages += pages
            total_input_tokens += (pages * TOKENS_PER_PAGE)

    results = {
        "request_count": request_count,
        "total_pages": total_pages,
        "total_input_tokens": total_input_tokens,
        "models": {}
    }

    for tier, rates in PRICING.items():
        if tier == "flash-lite":
            total_output_tokens = request_count * EST_OUTPUT_TOKENS_PER_CHUNK_LITE
        else:
            total_output_tokens = request_count * EST_OUTPUT_TOKENS_PER_CHUNK_FULL
            
        in_cost = total_input_tokens * rates["input"]
        out_cost = total_output_tokens * rates["output"]
        results["models"][tier] = {
            "input_cost": in_cost,
            "output_cost": out_cost,
            "total_cost": in_cost + out_cost,
            "est_output_tokens": total_output_tokens
        }
    
    return results

def print_estimation(results):
    if not results:
        print("No results to display.")
        return

    print("\n" + "="*55)
    print("BATCH COST ESTIMATION (2026 PRICING)")
    print("="*55)
    print(f"Total Requests:      {results['request_count']}")
    print(f"Total PDF Pages:     {results['total_pages']}")
    print(f"Total Input Tokens:  {results['total_input_tokens']:,.0f}")
    print("-" * 55)
    
    for tier, costs in results["models"].items():
        print(f"TIER: {tier.upper()}")
        print(f"  Est. Output Tokens: {costs['est_output_tokens']:,.0f}")
        print(f"  Input Cost:         ${costs['input_cost']:,.2f}")
        print(f"  Output Cost:        ${costs['output_cost']:,.2f}")
        print(f"  TOTAL EST:          ${costs['total_cost']:,.2f}")
        print("-" * 55)
    
    print("NOTE: Prices reflect Vertex AI Batch 50% discount in us-central1.")

if __name__ == "__main__":
    import sys
    path = DEFAULT_INPUT_JSONL
    if len(sys.argv) > 1:
        path = sys.argv[1]
    
    res = estimate_cost(path)
    print_estimation(res)
