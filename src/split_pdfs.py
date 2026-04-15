# -*- coding: utf-8 -*-
"""
split_pdfs.py (V2 - Refined Triage)
-------------
Physically splits large PDFs into smaller 'Micro-PDFs' containing only 
pages categorized as Golden, Mixed, or Continuation.
"""

import os
import fitz  # PyMuPDF
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRIAGE_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'fuzzy_triage_results.parquet')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'chunked_pdfs')
MAP_OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data', 'output', 'chunk_map.parquet')
PDF_LIBRARY_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"

# Target Categories to include in the batch
TARGET_CATEGORIES = ["Golden", "Mixed", "Continuation"]

# Gemini 1.5 Flash works well with chunks of ~15 pages
CHUNK_SIZE = 15

def split_target_files():
    if not os.path.exists(TRIAGE_PATH):
        print(f"Error: {TRIAGE_PATH} not found.")
        return

    print(f"Loading refined triage results from {TRIAGE_PATH}...")
    triage_full = pd.read_parquet(TRIAGE_PATH)
    
    # Filter by our target categories
    triage = triage_full[triage_full['triage_category'].isin(TARGET_CATEGORIES)].copy()
    
    print(f"Total target pages selected: {len(triage)}")
    print("Category Breakdown:")
    print(triage['triage_category'].value_counts())

    # Get unique files that have at least one target page
    target_files = triage[['set_name', 'filename']].drop_duplicates()
    print(f"Preparing to process {len(target_files)} unique PDFs...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    total_chunks_created = 0
    mapping_records = []

    # Load existing map if it exists for resumability
    existing_filenames = set()
    if os.path.exists(MAP_OUTPUT_PATH):
        try:
            existing_map = pd.read_parquet(MAP_OUTPUT_PATH)
            existing_filenames = set(existing_map['original_filename'].unique())
            print(f"Found {len(existing_filenames)} files already in chunk map. Resuming...")
        except:
            pass

    # Use tqdm for overall progress
    for _, row in tqdm(target_files.iterrows(), total=len(target_files), desc="Splitting PDFs"):
        sn = row['set_name']
        fn = row['filename']
        
        if fn in existing_filenames:
            continue
            
        pdf_path = os.path.join(PDF_LIBRARY_ROOT, sn, fn)
        if not os.path.exists(pdf_path):
            pdf_path = os.path.join(PDF_LIBRARY_ROOT, fn)
            
        if not os.path.exists(pdf_path):
            # print(f"  Warning: PDF not found at {pdf_path}")
            continue

        # Get target pages for this specific file (sorted 1-based)
        file_pages = sorted(triage[(triage['set_name']==sn) & (triage['filename']==fn)]['page_number'].tolist())
        
        if not file_pages:
            continue
            
        try:
            doc = fitz.open(pdf_path)
            # Create chunks of pages
            for i in range(0, len(file_pages), CHUNK_SIZE):
                chunk_pages = file_pages[i : i + CHUNK_SIZE]
                
                # PyMuPDF is 0-based
                zero_based_indices = [p - 1 for p in chunk_pages if (p - 1) < len(doc)]
                
                if not zero_based_indices: 
                    continue
                
                new_doc = fitz.open()
                for p_idx in zero_based_indices:
                    new_doc.insert_pdf(doc, from_page=p_idx, to_page=p_idx)
                
                # Unique name: Base filename + start index
                base_name = os.path.splitext(fn)[0]
                # Replace spaces and dots to be GCS-friendly
                safe_name = base_name.replace(" ", "_").replace(".", "_")
                chunk_filename = f"{safe_name}_v2_chunk_{i//CHUNK_SIZE}.pdf"
                chunk_path = os.path.join(OUTPUT_DIR, chunk_filename)
                
                new_doc.save(chunk_path)
                new_doc.close()
                
                # Record mapping for every page in this chunk
                for chunk_p_idx, original_p_num in enumerate(chunk_pages):
                    mapping_records.append({
                        "chunk_filename": chunk_filename,
                        "chunk_page": chunk_p_idx + 1, # 1-based
                        "original_page": original_p_num,
                        "original_filename": fn,
                        "set_name": sn
                    })

                total_chunks_created += 1
                
            doc.close()
            
        except Exception as e:
            print(f"  Error processing {fn}: {e}")

    # Save mapping to parquet for post-harvest reconstruction
    if mapping_records:
        new_map_df = pd.DataFrame(mapping_records)
        
        # If we have an existing map, load it and combine
        if os.path.exists(MAP_OUTPUT_PATH):
            try:
                full_map = pd.concat([pd.read_parquet(MAP_OUTPUT_PATH), new_map_df], ignore_index=True)
                full_map = full_map.drop_duplicates(subset=['chunk_filename', 'chunk_page'])
            except:
                full_map = new_map_df
        else:
            full_map = new_map_df
            
        full_map.to_parquet(MAP_OUTPUT_PATH, index=False)
        print(f"Saved chunk map to {MAP_OUTPUT_PATH} ({len(full_map)} total page mappings)")

    print(f"\nDone! Total Micro-PDFs created: {total_chunks_created}")
    return total_chunks_created

if __name__ == "__main__":
    split_target_files()
