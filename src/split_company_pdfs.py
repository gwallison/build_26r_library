# -*- coding: utf-8 -*-
"""
split_company_pdfs.py
---------------------
Extracts only the first page of files lacking Form 26R data for 
cost-efficient LLM company extraction.
"""

import os
import fitz  # PyMuPDF
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
F26R_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'all_harvested_form26r.parquet')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'company_pages')
PDF_LIBRARY_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"

def split_first_pages():
    print("Loading file lists...")
    corpus = pd.read_parquet(CORPUS_PATH, columns=['filename', 'page_number', 'set_name'])
    f26r = pd.read_parquet(F26R_PATH, columns=['filename'])
    
    files_with_f26r = set(f26r['filename'].unique())
    all_files = set(corpus['filename'].unique())
    target_files_list = sorted(list(all_files - files_with_f26r))
    
    # Get set_name mapping
    file_to_set = corpus[corpus['page_number'] == 1].set_index('filename')['set_name'].to_dict()

    print(f"Targeting {len(target_files_list)} files for page-1 extraction.")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    success_count = 0
    fail_count = 0
    
    for fn in tqdm(target_files_list, desc="Extracting Page 1"):
        sn = file_to_set.get(fn, "2010-2018")
        pdf_path = os.path.join(PDF_LIBRARY_ROOT, sn, fn)
        
        if not os.path.exists(pdf_path):
            pdf_path = os.path.join(PDF_LIBRARY_ROOT, fn)
            
        if not os.path.exists(pdf_path):
            fail_count += 1
            continue

        try:
            # We save it as {filename}_page1.pdf
            base_name = os.path.splitext(fn)[0]
            safe_name = base_name.replace(" ", "_").replace(".", "_")
            out_filename = f"{safe_name}_page1.pdf"
            out_path = os.path.join(OUTPUT_DIR, out_filename)
            
            if os.path.exists(out_path):
                success_count += 1
                continue

            doc = fitz.open(pdf_path)
            if doc.page_count > 0:
                new_doc = fitz.open()
                new_doc.insert_pdf(doc, from_page=0, to_page=0)
                new_doc.save(out_path)
                new_doc.close()
                success_count += 1
            else:
                fail_count += 1
            doc.close()
        except Exception as e:
            # print(f"Error processing {fn}: {e}")
            fail_count += 1

    print(f"\nExtraction complete.")
    print(f" - Successfully extracted: {success_count}")
    print(f" - Failed/Not found: {fail_count}")
    print(f"Output directory: {OUTPUT_DIR}")

if __name__ == "__main__":
    split_first_pages()
