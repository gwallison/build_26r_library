# -*- coding: utf-8 -*-
"""
split_pdfs.py
-------------
Physically splits large PDFs into smaller 'Micro-PDFs' containing only flagged pages.
Generates 'data/output/chunk_map.parquet' to track page mapping for post-harvest joining.
"""

import os
import fitz  # PyMuPDF
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_report_triage.parquet')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'chunked_pdfs')
MAP_OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'chunk_map.parquet')
PDF_LIBRARY_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
CHUNK_SIZE = 15

def split_target_files(target_df):
    """
    Splits only the files provided in the target_df (columns: set_name, filename).
    Saves a mapping of (chunk_file, chunk_page) -> original_page.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Loading triage metadata...")
    triage = pd.read_parquet(TRIAGE_PATH)

    print(f"Preparing to split {len(target_df)} target files...")
    
    total_chunks_created = 0
    mapping_records = []

    for _, row in target_df.iterrows():
        sn = row['set_name']
        fn = row['filename']
        
        pdf_path = os.path.join(PDF_LIBRARY_ROOT, sn, fn)
        if not os.path.exists(pdf_path):
            pdf_path = os.path.join(PDF_LIBRARY_ROOT, fn)
            
        if not os.path.exists(pdf_path):
            print(f"  Warning: PDF not found locally at {pdf_path}")
            continue

        # Get flagged pages for this file (1-based from triage)
        file_pages = sorted(triage[(triage['set_name']==sn) & (triage['filename']==fn)]['page_number'].tolist())
        
        if not file_pages:
            print(f"  Skipping {fn}: No triage pages found.")
            continue
            
        try:
            doc = fitz.open(pdf_path)
            for i in range(0, len(file_pages), CHUNK_SIZE):
                chunk_pages = file_pages[i : i + CHUNK_SIZE]
                
                # PyMuPDF uses 0-based page numbers
                zero_based_chunk = [p - 1 for p in chunk_pages if (p - 1) < len(doc)]
                
                if not zero_based_chunk: continue
                
                new_doc = fitz.open()
                for p_num in zero_based_chunk:
                    new_doc.insert_pdf(doc, from_page=p_num, to_page=p_num)
                
                base_name = os.path.splitext(fn)[0]
                chunk_filename = f"{base_name}_chunk_{i//CHUNK_SIZE}.pdf"
                chunk_path = os.path.join(OUTPUT_DIR, chunk_filename)
                
                new_doc.save(chunk_path)
                new_doc.close()
                
                # Record the mapping for every page in this chunk
                for chunk_p_idx, original_p_num in enumerate(chunk_pages):
                    mapping_records.append({
                        "chunk_filename": chunk_filename,
                        "chunk_page": chunk_p_idx + 1, # 1-based index within the Micro-PDF
                        "original_page": original_p_num,
                        "original_filename": fn
                    })

                total_chunks_created += 1
                
            doc.close()
            print(f"  Successfully chunked: {fn}")
            
        except Exception as e:
            print(f"  Error processing {fn}: {e}")

    # Save mapping
    if mapping_records:
        new_map_df = pd.DataFrame(mapping_records)
        if os.path.exists(MAP_OUTPUT_PATH):
            existing_map = pd.read_parquet(MAP_OUTPUT_PATH)
            # Append and drop duplicates
            final_map = pd.concat([existing_map, new_map_df]).drop_duplicates()
        else:
            final_map = new_map_df
        final_map.to_parquet(MAP_OUTPUT_PATH, index=False)
        print(f"Updated chunk map at {MAP_OUTPUT_PATH} ({len(final_map)} total page mappings)")

    print(f"\nDone! Total Micro-PDFs created: {total_chunks_created}")
    return total_chunks_created

if __name__ == "__main__":
    triage = pd.read_parquet(TRIAGE_PATH)
    merged_files = triage[['set_name', 'filename']].drop_duplicates().head(10)
    split_target_files(merged_files)
