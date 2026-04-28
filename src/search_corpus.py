import sqlite3
import sys
import argparse
import os

def search_corpus(query, db_path="data/corpus/corpus_search.db", limit=20, fuzzy=False, count_only=False, csv_output=None):
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    table = "pages_fuzzy" if fuzzy else "pages_idx"
    
    # 1. Handle Count Only
    if count_only:
        sql_count = f"SELECT count(*) FROM {table} WHERE text MATCH ?"
        try:
            count = cursor.execute(sql_count, (query,)).fetchone()[0]
            print(f"Total hits for '{query}': {count:,}")
        except sqlite3.OperationalError as e:
            print(f"Search Error: {e}")
        conn.close()
        return

    # 2. Regular Search
    # snippet(table, column_index, start, end, ellipses, num_tokens)
    # columns are: 0:filename, 1:set_name, 2:page_number, 3:text
    sql = f"""
    SELECT filename, set_name, page_number, snippet({table}, 3, '[[', ']]', '...', 15) as snippet
    FROM {table}
    WHERE text MATCH ?
    ORDER BY rank
    LIMIT ?
    """
    
    try:
        results = cursor.execute(sql, (query, limit)).fetchall()
        
        if not results:
            print(f"No results found for: {query}")
            return
            
        if csv_output:
            import csv
            with open(csv_output, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['filename', 'set_name', 'page_number', 'snippet'])
                writer.writerows(results)
            print(f"Saved {len(results)} results to {csv_output}")
        else:
            print(f"Top {len(results)} results for: {query}\n")
            # header
            print(f"{'Set Name':<20} {'Filename':<40} {'Pg':<4} {'Snippet'}")
            print("-" * 120)
            for row in results:
                fname, sname, page, snip = row
                snip = snip.replace('\n', ' ')
                print(f"{str(sname)[:18]:<20} {fname[:38]:<40} {str(page):<4} {snip}")
            
    except sqlite3.OperationalError as e:
        print(f"Search Error: {e}")
        print("\nTip: For NEAR queries use: NEAR(word1 word2, 5)")
        print("Tip: For boolean use: word1 AND word2")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search the PDF corpus index.")
    parser.add_argument("query", help="FTS5 query (e.g., 'Benzene AND Toluene', 'NEAR(Benzene Toluene, 10)', '\"Ground Water\"')")
    parser.add_argument("--limit", type=int, default=20, help="Number of results to show")
    parser.add_argument("--fuzzy", action="store_true", help="Use trigram fuzzy index (for OCR errors)")
    parser.add_argument("--count-only", action="store_true", help="Just return the total number of hits")
    parser.add_argument("--csv", help="Save results to a CSV file instead of printing")
    
    args = parser.parse_args()
    
    search_corpus(args.query, limit=args.limit, fuzzy=args.fuzzy, count_only=args.count_only, csv_output=args.csv)
