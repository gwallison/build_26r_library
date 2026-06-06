# -*- coding: utf-8 -*-
"""
build_lab_index.py
------------------
Creates a dedicated FTS5 SQLite search database for page-level lab reports.
Builds two tables:
  1. pages_idx (standard unicode61 tokenizer for fast word matching)
  2. pages_fuzzy (trigram tokenizer for typo-resilient matching)

Output:
    data/corpus/lab_report_corpus.db
"""

import os
import sqlite3
import time
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'lab_report_corpus.parquet')
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'lab_report_corpus.db')

def build_lab_index():
    if not os.path.exists(PARQUET_PATH):
        print(f"Error: Segmented parquet not found at {PARQUET_PATH}")
        print("Please run segment_lab_reports.py first.")
        return

    print(f"Opening {PARQUET_PATH}...")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"Loaded {len(df)} lab report pages.")

    import tempfile
    import shutil

    # We build the database in a local temp file first to prevent Google Drive sync conflicts
    temp_dir = tempfile.gettempdir()
    temp_db_path = os.path.join(temp_dir, 'lab_report_corpus.db')

    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)

    print(f"Building database locally at {temp_db_path}...")
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()

    # Optimizations for bulk insert
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = OFF")
    cursor.execute("PRAGMA cache_size = 100000")

    # 1. Create Standard FTS5 Virtual Table (unicode61)
    print("Creating Standard FTS5 table (pages_idx)...")
    cursor.execute("""
        CREATE VIRTUAL TABLE pages_idx USING fts5(
            filename,
            set_name UNINDEXED,
            page_number UNINDEXED,
            detected_lab UNINDEXED,
            triage_score UNINDEXED,
            lab_start_page UNINDEXED,
            lab_end_page UNINDEXED,
            text,
            tokenize = 'unicode61'
        );
    """)

    # 2. Create Fuzzy FTS5 Virtual Table (trigram)
    print("Creating Fuzzy FTS5 table (pages_fuzzy)...")
    cursor.execute("""
        CREATE VIRTUAL TABLE pages_fuzzy USING fts5(
            filename,
            set_name UNINDEXED,
            page_number UNINDEXED,
            detected_lab UNINDEXED,
            triage_score UNINDEXED,
            lab_start_page UNINDEXED,
            lab_end_page UNINDEXED,
            text,
            tokenize = 'trigram'
        );
    """)

    # Bulk insert
    batch_size = 10000
    start_time = time.time()

    # Insert into standard index
    print("Inserting pages into Standard index...")
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        cursor.executemany(
            """
            INSERT INTO pages_idx (filename, set_name, page_number, detected_lab, triage_score, lab_start_page, lab_end_page, text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch[['filename', 'set_name', 'page_number', 'detected_lab', 'triage_score', 'lab_start_page', 'lab_end_page', 'text']].values.tolist()
        )
        conn.commit()
        elapsed = time.time() - start_time
        print(f"  Processed {i + len(batch)}/{len(df)} pages... ({elapsed:.1f}s)")

    # Insert into fuzzy index
    print("Inserting pages into Fuzzy (Trigram) index...")
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        cursor.executemany(
            """
            INSERT INTO pages_fuzzy (filename, set_name, page_number, detected_lab, triage_score, lab_start_page, lab_end_page, text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch[['filename', 'set_name', 'page_number', 'detected_lab', 'triage_score', 'lab_start_page', 'lab_end_page', 'text']].values.tolist()
        )
        conn.commit()
        elapsed = time.time() - start_time
        print(f"  Processed {i + len(batch)}/{len(df)} pages... ({elapsed:.1f}s)")

    print("Optimizing indexes...")
    cursor.execute("INSERT INTO pages_idx(pages_idx) VALUES('optimize');")
    cursor.execute("INSERT INTO pages_fuzzy(pages_fuzzy) VALUES('optimize');")
    conn.commit()
    conn.close()

    if os.path.exists(DB_PATH):
        print(f"Removing existing database at {DB_PATH}...")
        os.remove(DB_PATH)

    print(f"Moving database to final destination: {DB_PATH}...")
    shutil.move(temp_db_path, DB_PATH)
    print(f"Done! Lab report search database successfully built at {DB_PATH}")

if __name__ == "__main__":
    build_lab_index()
