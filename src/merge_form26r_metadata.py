# -*- coding: utf-8 -*-
"""
merge_form26r_metadata.py
-------------------------
Post-processing script to join Form 26R metadata back into the extracted lab results.
Matches based on 'filename' and finds the most recent preceding F26R page.
"""

import pandas as pd
import os

def merge_metadata(samples_path, f26r_path, output_path):
    print(f"Merging F26R metadata from {f26r_path} into {samples_path}...")
    
    samples = pd.read_parquet(samples_path)
    f26r = pd.read_parquet(f26r_path)
    
    # Sort both for efficient search
    samples = samples.sort_values(['filename', 'pdf_page_number'])
    f26r = f26r.sort_values(['filename', 'page_number'])
    
    # Function to find the most recent preceding F26R
    def get_f26r_meta(row):
        file_f26r = f26r[f26r['filename'] == row['filename']]
        preceding = file_f26r[file_f26r['page_number'] < row['pdf_page_number']]
        if preceding.empty:
            return pd.Series([None]*4)
        last_f26r = preceding.iloc[-1]
        return pd.Series([
            last_f26r['company_name'],
            last_f26r['waste_location'],
            last_f26r['waste_code'],
            last_f26r['date_prepared']
        ])

    # Join
    # Note: Only apply if the columns don't already exist or are empty
    new_cols = ['joined_company', 'joined_location', 'joined_waste_code', 'joined_date']
    samples[new_cols] = samples.apply(get_f26r_meta, axis=1)
    
    samples.to_parquet(output_path, index=False)
    print(f"Saved merged results to: {output_path}")

if __name__ == "__main__":
    # Example usage (adjust paths after harvest)
    # merge_metadata("data/output/batch_harvest_surgical_v1/samples.parquet", 
    #                "data/output/all_harvested_form26r.parquet",
    #                "data/output/final_merged_data.parquet")
    pass
