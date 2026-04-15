import os
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'core_analytes_review.csv')
OUTPUT_CSV = os.path.join(PROJECT_ROOT, 'data', 'output', 'core_analytes_trimmed.csv')

def trim_analytes(limit=100, min_len=3):
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found.")
        return
        
    print(f"Loading analytes from {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV)
    
    # Ensure it's sorted by frequency
    df = df.sort_values(by='frequency', ascending=False)
    
    # Filter by minimum length (fuzzy regex constraint)
    df = df[df['clean_analyte'].str.len() >= min_len]
    
    # Take the top N
    trimmed_df = df.head(limit)
    
    print(f"Trimmed list to {len(trimmed_df)} analytes (min length: {min_len}).")
    
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    trimmed_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved trimmed analytes to {OUTPUT_CSV}")

if __name__ == "__main__":
    trim_analytes()
