import sqlite3
import sys
import argparse

def search_corpus(query, db_path="data/corpus/corpus_search.db", limit=20, fuzzy=False):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    table = "pages_fuzzy" if fuzzy else "pages_idx"
    
    sql = f"""
    SELECT filename, page_number, snippet({table}, 2, '[[', ']]', '...', 10) as snippet
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
            
        print(f"Top {len(results)} results for: {query}\n")
        print(f"{'Filename':<50} {'Page':<5} {'Snippet'}")
        print("-" * 110)
        for row in results:
            filename, page, snippet = row
            snippet = snippet.replace('\n', ' ')
            print(f"{filename[:48]:<50} {page:<5} {snippet}")
            
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
    
    args = parser.parse_args()
    
    search_corpus(args.query, limit=args.limit, fuzzy=args.fuzzy)
