import sqlite3
import pandas as pd
import argparse
import os

def batch_search(terms, db_path="data/corpus/corpus_search.db", output_path="data/output/search_hits.parquet"):
    """
    Searches the index for a list of terms and saves the filename/page hits.
    'terms' can be a list of strings.
    """
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    # Use a dictionary-like cursor for cleaner data extraction
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    all_hits = []
    
    print(f"Searching for {len(terms)} terms...")
    
    for term in terms:
        # We search and retrieve filename and page_number
        # We don't retrieve 'text' to keep the results lightweight
        sql = "SELECT filename, page_number FROM pages_idx WHERE text MATCH ?"
        
        try:
            # We wrap term in quotes for phrase matching if it contains spaces
            query = f'"{term}"' if ' ' in term and 'NEAR' not in term else term
            results = cursor.execute(sql, (query,)).fetchall()
            
            for row in results:
                all_hits.append({
                    "search_term": term,
                    "filename": row["filename"],
                    "page_number": row["page_number"]
                })
            
            print(f"  '{term}': {len(results)} hits")
            
        except sqlite3.OperationalError as e:
            print(f"  Error searching for '{term}': {e}")

    if all_hits:
        df_hits = pd.DataFrame(all_hits)
        
        # Save results
        if output_path.endswith(".parquet"):
            df_hits.to_parquet(output_path, index=False)
        else:
            df_hits.to_csv(output_path, index=False)
            
        print(f"\nSaved {len(all_hits)} total hits to {output_path}")
    else:
        print("No hits found for any terms.")
        
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch search terms in the PDF corpus.")
    parser.add_argument("--terms", nargs="+", help="Terms to search for (space separated)")
    parser.add_argument("--file", help="Path to a text file containing terms (one per line)")
    parser.add_argument("--output", default="data/output/batch_search_results.parquet", help="Output file path")
    
    args = parser.parse_args()
    
    search_list = []
    if args.terms:
        search_list.extend(args.terms)
    if args.file:
        if os.path.exists(args.file):
            with open(args.file, 'r') as f:
                search_list.extend([line.strip() for line in f if line.strip()])
        else:
            print(f"Error: File {args.file} not found.")
            
    if not search_list:
        print("No terms provided. Use --terms or --file.")
    else:
        batch_search(search_list, output_path=args.output)
