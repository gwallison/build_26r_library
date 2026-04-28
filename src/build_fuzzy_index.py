import sqlite3
import pandas as pd
import os
import time

def build_fuzzy_index(parquet_path, db_path):
    print(f"Opening {parquet_path} for fuzzy indexing...")
    df = pd.read_parquet(parquet_path, columns=['filename', 'page_number', 'text', 'set_name'])
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Optimizations for bulk insert
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = OFF")
    cursor.execute("PRAGMA cache_size = 100000")
    
    # Create FTS5 virtual table with TRIGRAM tokenizer
    # Note: Trigram is great for OCR errors but makes the index larger.
    print("Creating Fuzzy (Trigram) table...")
    cursor.execute("DROP TABLE IF EXISTS pages_fuzzy;")
    cursor.execute("""
        CREATE VIRTUAL TABLE pages_fuzzy USING fts5(
            filename,
            set_name UNINDEXED,
            page_number UNINDEXED,
            text,
            tokenize = 'trigram'
        );
    """)
    
    print(f"Inserting {len(df)} pages into fuzzy index...")
    start_time = time.time()
    
    batch_size = 10000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        cursor.executemany(
            "INSERT INTO pages_fuzzy (filename, set_name, page_number, text) VALUES (?, ?, ?, ?)",
            batch[['filename', 'set_name', 'page_number', 'text']].values.tolist()
        )
        conn.commit()
        elapsed = time.time() - start_time
        print(f"  Processed {i + len(batch)}/{len(df)} pages... ({elapsed:.1f}s)")

    # print("Optimizing fuzzy index...")
    # cursor.execute("INSERT INTO pages_fuzzy(pages_fuzzy) VALUES('optimize');")
    conn.commit()
    conn.close()
    
    print(f"Done! Fuzzy index added to {db_path}")

if __name__ == "__main__":
    parquet_file = "data/corpus/pdf_corpus.parquet"
    database_file = "data/corpus/corpus_search.db"
    build_fuzzy_index(parquet_file, database_file)
