# -*- coding: utf-8 -*-
"""
merge_26r_fer.py
----------------
Enriches the v3 Form 26R harvest with facility location data from linking_v3.

Join chain (all left joins, no row explosion):
  1. all_harvested_form26r_v3.parquet
       + forms_26r.parquet          on (filename, page_number)  -> cluster_id
  2.   + clusters_26r_fer.parquet   on cluster_id               -> anchor_pad_id, anchor_confidence, ...
  3.   + fer_registry.parquet       on anchor_pad_id            -> lat/lon, county, primary_operator, ...

Outputs:
    data/output/all_harvested_form26r_v3_fer.parquet
    data/output/Form_26R_files_v3_fer.html

Coverage (expected):
    ~73% of rows get a FER pad link (vs 60% from PA_FER_create_v2).
    anchor_confidence=0.57 rows have operator conflict — flag column is True.
"""

import os
import pandas as pd
import itables
from itables import init_notebook_mode
import itables.options as opt
from math import log10, floor

opt.classes = "display compact cell-border"
opt.buttons = ['pageLength', "copyHtml5", "csvHtml5"]
opt.maxBytes = 0
opt.allow_html = True
opt.lengthMenu = [2, 5, 10, 50, 100]
opt.pageLength = 5

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, 'data', 'output')

V3_PARQUET   = os.path.join(OUTPUT_DIR, 'all_harvested_form26r_v3.parquet')
OUT_PARQUET  = os.path.join(OUTPUT_DIR, 'all_harvested_form26r_v3_fer.parquet')
OUT_HTML     = os.path.join(OUTPUT_DIR, 'Form_26R_files_v3_fer.html')

LINKING_V3_DIR = r"G:\My Drive\sandbox\26R\linking_v3\data\output"
FER_V2_DIR     = r"G:\My Drive\sandbox\26R\PA_FER_create_v2\data\output"

FORMS_26R_PATH      = os.path.join(LINKING_V3_DIR, 'forms_26r.parquet')
CLUSTERS_FER_PATH   = os.path.join(LINKING_V3_DIR, 'clusters_26r_fer.parquet')
FER_REGISTRY_PATH   = os.path.join(FER_V2_DIR,     'fer_registry.parquet')

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def round_sig(x, sig=2, guarantee_str=''):
    try:
        if abs(x) >= 1:
            out = int(round(x, sig - int(floor(log10(abs(x)))) - 1))
            return f"{out:,d}"
        else:
            return round(x, sig - int(floor(log10(abs(x)))) - 1)
    except Exception:
        return guarantee_str if guarantee_str else x


def get_pdf_url(row):
    rooturl = "https://storage.googleapis.com/fta-form26r-library/full-set/"
    fn = row.filename.replace(' ', '%20')
    pn = f'#page={row.page_number}'
    return f"{rooturl}{row.set_name}/{fn}{pn}"


def get_link(row):
    url = get_pdf_url(row)
    return f"""<a href={url} target="_blank">Open PDF</a>"""


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

def build_merged(v3_path, forms_path, clusters_fer_path, fer_registry_path):
    print("Loading v3 harvest...")
    v3 = pd.read_parquet(v3_path)
    print(f"  {len(v3):,} rows")

    # Step 1: attach cluster_id from linking_v3 forms table
    print("Loading linking_v3 forms (cluster assignments)...")
    forms = pd.read_parquet(forms_path, columns=['filename', 'page_number', 'cluster_id'])
    v3 = v3.merge(forms, on=['filename', 'page_number'], how='left')
    missing = v3['cluster_id'].isna().sum()
    if missing:
        print(f"  WARNING: {missing} rows could not be assigned a cluster_id")

    # Step 2: attach FER anchor from linking_v3 clusters table
    print("Loading linking_v3 cluster FER anchors...")
    anchor_cols = ['cluster_id', 'anchor_pad_id', 'anchor_method',
                   'anchor_confidence', 'anchor_flagged', 'anchor_op_agree']
    clusters_fer = pd.read_parquet(clusters_fer_path, columns=anchor_cols)
    v3 = v3.merge(clusters_fer, on='cluster_id', how='left')
    anchored = v3['anchor_pad_id'].notna().sum()
    print(f"  Anchored: {anchored:,} / {len(v3):,} rows ({100*anchored/len(v3):.1f}%)")

    # Step 3: attach geocoded fields from FER registry
    print("Loading FER registry (geocoded pad data)...")
    fer_keep = ['WELL_PAD_ID', 'WELL_PAD', 'WELL_PAD_LATITUDE', 'WELL_PAD_LONGITUDE',
                'COUNTY', 'MUNICIPALITY', 'ADDRESS1', 'CITY', 'ZIP',
                'primary_operator', 'well_count']
    fer_reg = pd.read_parquet(fer_registry_path, columns=fer_keep)
    # Rename to avoid collision with form's 'address' column
    fer_reg = fer_reg.rename(columns={'ADDRESS1': 'pad_address',
                                      'CITY':     'pad_city',
                                      'ZIP':      'pad_zip'})
    v3 = v3.merge(fer_reg, left_on='anchor_pad_id', right_on='WELL_PAD_ID', how='left')
    v3 = v3.drop(columns=['WELL_PAD_ID'])  # redundant with anchor_pad_id

    with_latlon = v3['WELL_PAD_LATITUDE'].notna().sum()
    print(f"  With lat/lon: {with_latlon:,} / {len(v3):,} rows ({100*with_latlon/len(v3):.1f}%)")

    # Confidence tier summary
    print("\n  Confidence tier breakdown:")
    tiers = [
        ('high   (>=0.87)', v3['anchor_confidence'] >= 0.87),
        ('medium (0.77)',    v3['anchor_confidence'] == 0.77),
        ('low    (0.60-0.65)', v3['anchor_confidence'].between(0.60, 0.65)),
        ('conflict (0.57)', v3['anchor_confidence'] == 0.57),
        ('none   (0.00)',   v3['anchor_confidence'] == 0.00),
    ]
    for label, mask in tiers:
        n = int(mask.sum())
        ll = int((mask & v3['WELL_PAD_LATITUDE'].notna()).sum())
        print(f"    {label}: {n:>6,} rows  ({ll:,} with lat/lon)")

    return v3


# ---------------------------------------------------------------------------
# HTML table
# ---------------------------------------------------------------------------

table_styling = """<style>
  body {
    font-family: "Noto Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell,
                 "Fira Sans", "Droid Sans", "Helvetica Neue", sans-serif;
    font-size: 16px;
    color: rgb(9, 66, 100);
  }
  table, th, td { border-color: rgb(214, 239, 238); }
  th { font-weight: 400; }
</style>"""


def make_html(df, out_path):
    init_notebook_mode(all_interactive=True, connected=True)

    df = df.copy()
    df['pdf_link'] = df.apply(get_link, axis=1)
    df['url']      = df.apply(get_pdf_url, axis=1)
    df['volume_shipped'] = df['volume_shipped'].map(lambda x: round_sig(x, 3))

    display_cols = [
        'pdf_link', 'page_number',
        'company_name', 'waste_location', 'date_prepared',
        'WELL_PAD', 'COUNTY', 'MUNICIPALITY',
        'WELL_PAD_LATITUDE', 'WELL_PAD_LONGITUDE',
        'anchor_method', 'anchor_confidence', 'anchor_flagged',
        'primary_operator',
        'waste_code', 'waste_description',
        'facility_name', 'address', 'volume_shipped', 'volume_unit',
        'filename', 'set_name', 'url',
    ]

    html = itables.to_html_datatable(
        df[display_cols].reset_index(drop=True),
        connected=True,
        pageLength=10,
        display_logo_when_loading=False,
        lengthMenu=[2, 5, 10, 50, 100],
        buttons=['pageLength', 'copyHtml5', 'csvHtml5', 'colvis'],
        column_filters="footer",
        footer=True,
        maxBytes=0,
    )

    title = """
    <div class="title-container">
    <h2>All files with Form 26R — FER Enriched</h2>
    <h3>Each row is a DESTINATION on the form.</h3>
    Pad location (lat/lon, county, municipality) sourced from PA DEP well pad registry
    via linking_v3 cluster anchoring. anchor_flagged=True indicates operator conflict.
    </div>
"""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"<html><head>{table_styling}</head><body>")
        f.write(title + html)
        f.write("</body></html>")
    print(f"HTML written to {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    merged = build_merged(
        v3_path=V3_PARQUET,
        forms_path=FORMS_26R_PATH,
        clusters_fer_path=CLUSTERS_FER_PATH,
        fer_registry_path=FER_REGISTRY_PATH,
    )

    print(f"\nWriting parquet ({len(merged):,} rows, {len(merged.columns)} columns)...")
    merged.to_parquet(OUT_PARQUET, index=False)
    print(f"Parquet written to {OUT_PARQUET}")

    print("Building HTML table...")
    make_html(merged, OUT_HTML)
    print("Done.")
