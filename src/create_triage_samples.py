import os
import fitz  # PyMuPDF
import pandas as pd
import random
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_RESULTS_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'fuzzy_triage_results.parquet')
PDF_LIBRARY_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
OUTPUT_BASE_DIR = os.path.join(PROJECT_ROOT, 'data', 'triage_samples')

SAMPLES_PER_CATEGORY = 200

def create_samples():
    if not os.path.exists(TRIAGE_RESULTS_PATH):
        print(f"Error: {TRIAGE_RESULTS_PATH} not found.")
        return

    print(f"Loading triage results from {TRIAGE_RESULTS_PATH}...")
    df = pd.read_parquet(TRIAGE_RESULTS_PATH)
    
    categories = df['triage_category'].unique()
    print(f"Found categories: {categories}")

    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    for category in categories:
        if category == "Noise": continue # We already excluded noise
        
        print(f"\nProcessing category: {category}...")
        cat_df = df[df['triage_category'] == category]
        
        # Sample pages
        sample_count = min(len(cat_df), SAMPLES_PER_CATEGORY)
        samples = cat_df.sample(n=sample_count, random_state=42)
        
        cat_dir = os.path.join(OUTPUT_BASE_DIR, category)
        os.makedirs(cat_dir, exist_ok=True)
        
        success_count = 0
        log_records = []

        for _, row in tqdm(samples.iterrows(), total=sample_count, desc=f"Extracting {category}"):
            sn = row['set_name']
            fn = row['filename']
            pn = row['page_number'] # 1-based
            
            # Try to find the PDF
            pdf_path = os.path.join(PDF_LIBRARY_ROOT, sn, fn)
            if not os.path.exists(pdf_path):
                # Try fallback to just filename if set_name is missing or wrong
                pdf_path = os.path.join(PDF_LIBRARY_ROOT, fn)
                
            if not os.path.exists(pdf_path):
                continue
                
            try:
                doc = fitz.open(pdf_path)
                # pn is 1-based, PyMuPDF is 0-based
                if pn <= len(doc):
                    new_doc = fitz.open()
                    new_doc.insert_pdf(doc, from_page=pn-1, to_page=pn-1)
                    
                    output_filename = f"{os.path.splitext(fn)[0]}_p{pn}.pdf"
                    output_path = os.path.join(cat_dir, output_filename)
                    
                    new_doc.save(output_path)
                    new_doc.close()
                    
                    log_records.append({
                        "sample_filename": output_filename,
                        "original_file": fn,
                        "original_page": pn,
                        "match_count": row['match_count'],
                        "is_lab_control": row['is_lab_control'],
                        "has_pos_marker": row['has_pos_marker']
                    })
                    success_count += 1
                doc.close()
            except Exception as e:
                # print(f"Error extracting {fn} p{pn}: {e}")
                pass
        
        print(f"Successfully extracted {success_count} samples for {category}.")
        
        # Save log for this category
        if log_records:
            log_df = pd.DataFrame(log_records)
            log_df.to_csv(os.path.join(cat_dir, "_sample_log.csv"), index=False)

    print(f"\nAll samples created in {OUTPUT_BASE_DIR}")

if __name__ == "__main__":
    create_samples()
