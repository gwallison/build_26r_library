import sqlite3
import argparse
import os
import sys
import time
import re
from rapidfuzz import process, fuzz

def get_single_word_variants(term, words, score_cutoff=80, limit=20):
    matches = process.extract(term.lower(), words, scorer=fuzz.ratio, limit=limit, score_cutoff=score_cutoff)
    return [{"variant": m[0], "score": round(m[1], 1)} for m in matches]

def get_variants(query_terms, db_path="data/corpus/corpus_search.db", score_cutoff=80, limit=20):
    if not os.path.exists(db_path):
        return {}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("CREATE VIRTUAL TABLE IF NOT EXISTS temp.vocab USING fts5vocab('main', 'pages_idx', 'row')")
        words = [r[0] for r in cursor.execute("SELECT term FROM temp.vocab WHERE term GLOB '[a-zA-Z]*'").fetchall() if len(r[0]) >= 3]
        
        results = {}
        for query in query_terms:
            parts = query.split()
            if len(parts) == 1:
                results[query] = {
                    "type": "word",
                    "variants": get_single_word_variants(query, words, score_cutoff=score_cutoff, limit=limit)
                }
            else:
                print(f"Finding variants for phrase '{query}'...")
                
                # 1. Joined Variants (Search for single tokens like "rangeresources")
                joined_query = "".join(parts)
                joined_variants = get_single_word_variants(joined_query, words, score_cutoff=score_cutoff, limit=limit)
                
                # 2. Phrase variants via proximity search (on-page matches like "Range Resour")
                part1_vars = [v['variant'] for v in get_single_word_variants(parts[0], words, score_cutoff=score_cutoff, limit=5)]
                part2_vars = [v['variant'] for v in get_single_word_variants(parts[1], words, score_cutoff=score_cutoff, limit=5)]
                
                q1 = "(" + " OR ".join(part1_vars) + ")"
                q2 = "(" + " OR ".join(part2_vars) + ")"
                combined_query = f"{q1} AND {q2}"
                
                sql = "SELECT snippet(pages_fuzzy, 3, '[[', ']]', '', 10) FROM pages_fuzzy WHERE text MATCH ? LIMIT 100"
                hits = cursor.execute(sql, (combined_query,)).fetchall()
                
                on_page_variants = {}
                for (snip,) in hits:
                    first = snip.find('[[')
                    last = snip.rfind(']]')
                    if first != -1 and last != -1 and (last - first) < 50:
                        span = snip[first+2:last].replace('[[', '').replace(']]', '').strip().replace('\n', ' ')
                        if len(span.split()) >= 2:
                            on_page_variants[span] = on_page_variants.get(span, 0) + 1
                
                results[query] = {
                    "type": "phrase",
                    "joined_variants": joined_variants,
                    "on_page_variants": [{"variant": k, "hits": v} for k, v in sorted(on_page_variants.items(), key=lambda x: x[1], reverse=True)]
                }
                
        return results
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("terms", nargs="+")
    parser.add_argument("--cutoff", type=int, default=80)
    parser.add_argument("--csv", help="Save results to a CSV file")
    args = parser.parse_args()
    
    print(f"Scanning vocabulary for variants of: {', '.join(args.terms)}...")
    start_time = time.time()
    res = get_variants(args.terms, score_cutoff=args.cutoff)
    elapsed = time.time() - start_time

    if args.csv:
        import csv
        with open(args.csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['query_term', 'match_type', 'variant', 'score', 'hits'])
            for term, data in res.items():
                if data['type'] == "word":
                    for m in data['variants']:
                        writer.writerow([term, 'word', m['variant'], m['score'], ''])
                else:
                    for m in data['joined_variants']:
                        writer.writerow([term, 'joined', m['variant'], m['score'], ''])
                    for m in data['on_page_variants']:
                        writer.writerow([term, 'proximity', m['variant'], '', m['hits']])
        print(f"Saved {len(res)} term results to {args.csv}")
    else:
        for term, data in res.items():
            print(f"\n{'='*60}")
            print(f"RESULTS FOR: '{term}'")
            print(f"{'='*60}")
            
            if data['type'] == "word":
                for m in data['variants']:
                    print(f"  - {m['variant']:<30} (Score: {m['score']})")
            else:
                # Joined Phrase first
                print("\nJoined Variants (Single-word matches in dictionary):")
                if not data['joined_variants']:
                    print("  (No joined variants found)")
                for m in data['joined_variants']:
                    print(f"  - {m['variant']:<30} (Score: {m['score']})")
                    
                # Then the on-page proximity matches
                print("\nProximity Variants (Multi-word matches on pages):")
                if not data['on_page_variants']:
                    print("  (No multi-word variants found)")
                for m in data['on_page_variants']:
                    print(f"  - {m['variant']:<30} ({m['hits']} hits)")
    
    print(f"\nDone in {elapsed:.2f}s")
