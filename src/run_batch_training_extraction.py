# -*- coding: utf-8 -*-
"""
run_batch_training_extraction.py
--------------------------------
Iterates through all PDF files in data/training_examples/ and runs 
the Gemini extraction for each.

Usage:
    python src/run_batch_training_extraction.py
"""

import os
import glob
from run_training_extraction import run_training_extraction

def run_batch():
    # Resolve project root and training directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    training_dir = os.path.join(project_root, 'data', 'training_examples')

    if not os.path.isdir(training_dir):
        print(f"Error: Training directory not found: {training_dir}")
        return

    # Find all PDF files in the training directory
    pdf_files = glob.glob(os.path.join(training_dir, "*.pdf"))
    
    # Filter for files starting with "training_" to avoid re-processing 
    # or processing other PDFs if they exist.
    training_pdfs = [f for f in pdf_files if os.path.basename(f).startswith("training_")]

    if not training_pdfs:
        print(f"No files starting with 'training_' found in {training_dir}")
        return

    print(f"Found {len(training_pdfs)} training PDFs. Starting batch extraction...\n")

    for i, pdf_path in enumerate(training_pdfs, 1):
        filename = os.path.basename(pdf_path)
        print(f"[{i}/{len(training_pdfs)}] Processing {filename}...")
        
        try:
            run_training_extraction(pdf_path)
        except Exception as e:
            print(f"Error processing {filename}: {e}")
        
        print("-" * 40)

    print("\nBatch extraction complete.")

if __name__ == "__main__":
    run_batch()
