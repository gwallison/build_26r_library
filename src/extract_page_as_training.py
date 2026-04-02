"""
Utility to extract a single page from a PDF and save it as a separate file.
Usage:
    from src.extract_page_as_training import extract_single_page
    extract_single_page("path/to/my.pdf", 5)  # extracts page 5 (1-indexed)
"""

import os
import fitz  # PyMuPDF

def extract_single_page(pdf_path: str, page_number: int, output_dir: str = None) -> str:
    """
    Extracts a single page (1-indexed) from a PDF file and writes it to a new file.
    
    Args:
        pdf_path: Path to the source PDF file.
        page_number: The page number to extract (1st page is 1).
        output_dir: Optional override for the output directory. 
                    Defaults to project_root/data/training_examples.

    Returns:
        The path to the newly created PDF file.
    """
    if not os.path.isfile(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    # Resolve project root and output directory
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if output_dir is None:
        output_dir = os.path.join(project_root, 'data', 'training_examples')

    os.makedirs(output_dir, exist_ok=True)

    # Prepare output filename
    base_name = os.path.basename(pdf_path)
    output_filename = f"training_{base_name}"
    output_path = os.path.join(output_dir, output_filename)

    # Open the source PDF
    src_doc = fitz.open(pdf_path)
    
    # Check page range
    total_pages = src_doc.page_count
    if page_number < 1 or page_number > total_pages:
        src_doc.close()
        raise ValueError(f"Invalid page number {page_number}. PDF has {total_pages} pages.")

    # Create a new PDF for the single page
    dest_doc = fitz.open()
    # insert_pdf takes 0-indexed page numbers (from, to)
    dest_doc.insert_pdf(src_doc, from_page=page_number-1, to_page=page_number-1)

    # Save the output file
    dest_doc.save(output_path)

    # Close the documents
    src_doc.close()
    dest_doc.close()

    return output_path

if __name__ == "__main__":
    # Example usage for testing
    import sys
    if len(sys.argv) > 2:
        path = sys.argv[1]
        pnum = int(sys.argv[2])
        try:
            out = extract_single_page(path, pnum)
            print(f"Successfully extracted page {pnum} to: {out}")
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("Usage: python src/extract_page_as_training.py <pdf_path> <page_number>")
