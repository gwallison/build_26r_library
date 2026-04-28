import sqlite3
import pandas as pd
import os
import time

def build_index(parquet_path, db_path):
    print(f"Opening {parquet_path}...")
    # Use pandas to read the parquet file. 
    # For very large files, we could use fastparquet or pyarrow directly to stream chunks.
    df = pd.read_parquet(parquet_path, columns=['filename', 'page_number', 'text', 'set_name'])
    
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Optimizations for bulk insert
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = OFF")
    cursor.execute("PRAGMA cache_size = 100000")
    
    # Create FTS5 virtual table
    # page_number and set_name are UNINDEXED because we don't search BY them, we just retrieve them.
    print("Creating FTS5 table...")
    cursor.execute("""
        CREATE VIRTUAL TABLE pages_idx USING fts5(
            filename,
            set_name UNINDEXED,
            page_number UNINDEXED,
            text,
            tokenize = 'unicode61'
        );
    """)
    
    print(f"Inserting {len(df)} pages into index...")
    start_time = time.time()
    
    # Insert in batches for performance
    batch_size = 10000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        cursor.executemany(
            "INSERT INTO pages_idx (filename, set_name, page_number, text) VALUES (?, ?, ?, ?)",
            batch[['filename', 'set_name', 'page_number', 'text']].values.tolist()
        )
        conn.commit()
        elapsed = time.time() - start_time
        print(f"  Processed {i + len(batch)}/{len(df)} pages... ({elapsed:.1f}s)")

    print("Optimizing index...")
    cursor.execute("INSERT INTO pages_idx(pages_idx) VALUES('optimize');")
    conn.commit()
    conn.close()
    
    print(f"Done! Index saved to {db_path}")

if __name__ == "__main__":
    parquet_file = "data/corpus/pdf_corpus.parquet"
    database_file = "data/corpus/corpus_search.db"
    build_index(parquet_file, database_file)
