import pandas as pd
import re

file_path = 'data/output/batch_cleaned_vertex/batch_results_cleaned.parquet'
df = pd.read_parquet(file_path)

def is_bad(val):
    if val is None or pd.isna(val) or val == "": return False
    # Remove common numeric modifiers
    clean = re.sub(r'[<>\s\+/-]', '', str(val))
    try:
        float(clean)
        return False
    except:
        # It's bad if it's not a common placeholder
        return str(val).upper() not in ['ND', 'NAN', 'NONE', '', 'PASS', 'FAIL', 'ABSENT']

bad_rl = df[df.reporting_limit.apply(is_bad)]
bad_mdl = df[df.mdl.apply(is_bad)]

print(f"Total Rows: {len(df)}")
print(f"Rows with non-numeric RL: {len(bad_rl)}")
print(f"Rows with non-numeric MDL: {len(bad_mdl)}")

if len(bad_rl) > 0:
    print("\nExamples of bad RL:")
    print(bad_rl[['reporting_limit', 'mdl', 'units', 'raw_csv_output']].head(20).to_string())

if len(bad_mdl) > 0:
    print("\nExamples of bad MDL:")
    print(bad_mdl[['mdl', 'units', 'raw_csv_output']].head(20).to_string())
