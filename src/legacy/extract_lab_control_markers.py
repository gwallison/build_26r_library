import os
import pandas as pd
import re
from collections import Counter
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORPUS_PATH = os.path.join(PROJECT_ROOT, 'data', 'corpus', 'pdf_corpus.parquet')
OUTPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'lab_control_markers_candidates.csv')

# Known triggers for Lab Control pages
TRIGGERS = [
    r"Method Blank",
    r"Blank Spike",
    r"LCS",
    r"Matrix Spike",
    r"Surrogate",
    r"Laboratory Control Sample",
    r"Duplicate",
    r"Post Digestion Spike"
]

def sample_lab_control_terms(sample_size=10000):
    if not os.path.exists(CORPUS_PATH):
        print(f"Error: {CORPUS_PATH} not found.")
        return
        
    print(f"Sampling {sample_size} pages from {CORPUS_PATH} to find LC markers...")
    
    # Read text column only
    df = pd.read_parquet(CORPUS_PATH, columns=['text'])
    
    # Sample if necessary
    if len(df) > sample_size:
        sample_df = df.sample(sample_size, random_state=42)
    else:
        sample_df = df

    # Find pages that hit at least one trigger
    trigger_regex = re.compile("|".join(TRIGGERS), re.IGNORECASE)
    lc_pages = sample_df[sample_df['text'].str.contains(trigger_regex, na=False)]
    
    print(f"Found {len(lc_pages)} potential LC pages out of {len(sample_df)} sampled.")
    
    # Extract common n-grams (2-3 words) from these pages
    words_counter = Counter()
    
    for text in tqdm(lc_pages['text'], desc="Extracting candidate markers"):
        # Clean text: keep only letters/spaces, lower case
        text = re.sub(r'[^a-zA-Z\s]', ' ', text.lower())
        words = text.split()
        
        # 2-grams
        for i in range(len(words)-1):
            gram = f"{words[i]} {words[i+1]}"
            if len(gram) > 5: # ignore very short junk
                words_counter[gram] += 1
                
        # 3-grams
        for i in range(len(words)-2):
            gram = f"{words[i]} {words[i+1]} {words[i+2]}"
            if len(gram) > 8:
                words_counter[gram] += 1

    # Get most common
    common = words_counter.most_common(200)
    results_df = pd.DataFrame(common, columns=['term', 'frequency'])
    
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    results_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved candidate markers to {OUTPUT_CSV}")

if __name__ == "__main__":
    sample_lab_control_terms()
