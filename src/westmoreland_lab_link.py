# -*- coding: utf-8 -*-
"""
westmoreland_lab_link.py
-------------------------
First-pass linkage of lab report results to Westmoreland Landfill 26R sites.

Linkage logic (within files present in both datasets):
  Path A — f26r_location match: lab row's f26r_location matches a Westmoreland
            waste_location in the same PDF. Highest confidence; works for both
            single-pad files and large annual reports where the extractor already
            identified the nearby 26R context.
  Path B — page proximity: lab row has no matching f26r_location but the
            original_page > some Westmoreland form page in the same file.
            Linked to the nearest preceding Westmoreland form (largest form
            page_number < original_page). Lower confidence.

Both paths require: page_number (26R) < original_page (lab).

Outputs:
    data/output/westmoreland_lab_links.parquet   — full linked result rows
    data/output/westmoreland_lab_links.csv       — same, CSV
    data/output/westmoreland_lab_summary.html    — site × analyte summary table
"""

import os
import bisect
import re
import pandas as pd
import itables
from itables import init_notebook_mode

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, 'data', 'output')

MATCHES_CSV  = r"G:\My Drive\sandbox\26R\data_cleanup_26R\processed\westmoreland_landfill_f26r_matches.csv"
LAB_PARQUET  = r"G:\My Drive\sandbox\26R\data_cleanup_26R\processed\lab_results_result_parsed.parquet"

OUT_PARQUET  = os.path.join(OUTPUT_DIR, 'westmoreland_lab_links.parquet')
OUT_CSV      = os.path.join(OUTPUT_DIR, 'westmoreland_lab_links.csv')
OUT_HTML     = os.path.join(OUTPUT_DIR, 'westmoreland_lab_summary.html')

# ---------------------------------------------------------------------------
# Site name normalization (shared with other Westmoreland scripts)
# ---------------------------------------------------------------------------

_WELLPAD_SUFFIX = re.compile(r'\s*[-–]?\s*well\s*pad\b.*$', re.I)

def canonical_site(name):
    if pd.isna(name):
        return ''
    name = str(name).strip()
    name = re.sub(r'\s{2,}.*$', '', name)
    name = _WELLPAD_SUFFIX.sub('', name).strip()
    return re.sub(r'\s+', ' ', name)

# ---------------------------------------------------------------------------
# Link lab results to Westmoreland 26R forms
# ---------------------------------------------------------------------------

def link_lab_to_26r(matches_df, lab_df):
    """
    Returns a DataFrame of lab result rows linked to a Westmoreland 26R form.
    Adds columns: page_number, waste_location, waste_code, site, match_path.
    """
    overlap = set(matches_df['filename'].unique()) & set(lab_df['original_filename'].unique())
    print(f"Files in both datasets: {len(overlap)}")

    lab_sub = lab_df[lab_df['original_filename'].isin(overlap)].copy()

    # Build a per-file lookup: filename -> sorted list of (page_number, waste_location, waste_code)
    form_index = {}
    for fname, grp in matches_df[matches_df['filename'].isin(overlap)].groupby('filename'):
        rows = grp[['page_number', 'waste_location', 'waste_code']].drop_duplicates()
        form_index[fname] = rows.sort_values('page_number')

    # Build a lookup: (filename, waste_location) -> page_number
    # for Path A f26r_location matching
    loc_to_page = {}
    for fname, rows in form_index.items():
        for _, r in rows.iterrows():
            loc_to_page[(fname, r['waste_location'])] = r['page_number']

    linked_records = []

    for fname, lab_grp in lab_sub.groupby('original_filename'):
        form_rows = form_index[fname]
        form_pages = sorted(form_rows['page_number'].tolist())

        # Build map: page_number -> (waste_location, waste_code) for this file
        page_to_wl = {
            r['page_number']: (r['waste_location'], r['waste_code'])
            for _, r in form_rows.iterrows()
        }

        for _, lab_row in lab_grp.iterrows():
            lab_page = lab_row['original_page']
            f26r_loc = lab_row.get('f26r_location', '')

            match_path = None
            form_page  = None
            waste_loc  = None
            waste_code = None

            # --- Path A: f26r_location match ---
            if pd.notna(f26r_loc) and f26r_loc:
                key = (fname, f26r_loc)
                if key in loc_to_page:
                    fp = loc_to_page[key]
                    if fp < lab_page:            # sanity: form must precede lab
                        form_page  = fp
                        waste_loc  = f26r_loc
                        waste_code = page_to_wl.get(fp, (None, None))[1]
                        match_path = 'f26r_location'

            # --- Path B: page proximity fallback ---
            if match_path is None:
                idx = bisect.bisect_right(form_pages, lab_page) - 1
                if idx >= 0:
                    fp = form_pages[idx]
                    if fp < lab_page:
                        form_page  = fp
                        waste_loc, waste_code = page_to_wl[fp]
                        match_path = 'page_proximity'

            if match_path is None:
                continue   # no valid link found

            rec = lab_row.to_dict()
            rec['page_number'] = form_page
            rec['waste_location'] = waste_loc
            rec['waste_code_26r'] = waste_code
            rec['site'] = canonical_site(waste_loc)
            rec['match_path'] = match_path
            linked_records.append(rec)

    return pd.DataFrame(linked_records)


# ---------------------------------------------------------------------------
# Summary HTML table (site × analyte)
# ---------------------------------------------------------------------------

table_styling = """<style>
  body {
    font-family: "Noto Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 14px; color: rgb(9, 66, 100);
  }
  table, th, td { border-color: rgb(214, 239, 238); }
  th { font-weight: 500; }
</style>"""


def make_summary_html(linked, out_path):
    """One row per (site, analyte_norm) showing result statistics."""
    init_notebook_mode(all_interactive=True, connected=True)

    # Keep only rows with a numeric result
    num = linked[linked['result_value'].notna()].copy()

    def first_mode(series):
        m = series.dropna().mode()
        return m.iloc[0] if len(m) > 0 else ''

    def agg(g):
        return pd.Series({
            'n_results':    len(g),
            'n_detect':     (g['result_flag'] != '<').sum(),
            'min':          g['result_value'].min(),
            'median':       g['result_value'].median(),
            'max':          g['result_value'].max(),
            'units':        first_mode(g['units']),
            'lab_names':    ', '.join(sorted(g['lab_name'].dropna().unique())),
            'match_paths':  ', '.join(sorted(g['match_path'].unique())),
            'waste_codes':  ', '.join(sorted(g['waste_code_26r'].dropna().astype(str).unique())),
            'collection_dates': ', '.join(sorted(g['collection_date'].dropna().unique())[:5]),
        })

    summary = (
        num.groupby(['site', 'analyte_norm'])
        .apply(agg, include_groups=False)
        .reset_index()
    )
    summary = summary.sort_values(['site', 'analyte_norm'])

    html = itables.to_html_datatable(
        summary.reset_index(drop=True),
        connected=True,
        pageLength=25,
        display_logo_when_loading=False,
        lengthMenu=[10, 25, 50, 100],
        buttons=['pageLength', 'copyHtml5', 'csvHtml5', 'colvis'],
        column_filters="footer",
        footer=True,
    )

    title = """
    <div style="padding:12px 0;">
      <h2 style="margin-bottom:4px;">Lab Results — Westmoreland Landfill Sites</h2>
      <p style="margin:0; color:#555;">
        One row per site × analyte. Results linked to Westmoreland 26R forms via
        f26r_location match (Path A) or nearest-preceding-form page proximity (Path B).
        Only numeric results shown; &lt;MDL detections counted separately.
      </p>
    </div>
"""

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"<html><head>{table_styling}</head><body>")
        f.write(title + html)
        f.write("</body></html>")
    print(f"Summary HTML written to {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Loading Westmoreland 26R matches...")
    matches = pd.read_csv(MATCHES_CSV)
    print(f"  {len(matches)} rows across {matches['filename'].nunique()} files")

    print("Loading lab results...")
    lab = pd.read_parquet(LAB_PARQUET)
    print(f"  {len(lab):,} rows across {lab['original_filename'].nunique():,} files")

    print("\nLinking lab results to Westmoreland 26R forms...")
    linked = link_lab_to_26r(matches, lab)
    print(f"\nLinked rows: {len(linked):,}")

    # Coverage report
    path_counts = linked['match_path'].value_counts()
    print(f"  Path A (f26r_location match):  {path_counts.get('f26r_location', 0):,}")
    print(f"  Path B (page proximity):        {path_counts.get('page_proximity', 0):,}")

    site_counts = linked['site'].value_counts()
    print(f"\nSites with lab results: {len(site_counts)}")
    print(site_counts.to_string())

    # Save outputs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    linked.to_parquet(OUT_PARQUET, index=False)
    print(f"\nParquet written to {OUT_PARQUET}")
    linked.to_csv(OUT_CSV, index=False)
    print(f"CSV written to {OUT_CSV}")

    print("\nBuilding summary HTML...")
    make_summary_html(linked, OUT_HTML)
    print("Done.")
