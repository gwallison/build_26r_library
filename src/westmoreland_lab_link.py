# -*- coding: utf-8 -*-
"""
westmoreland_lab_link.py
-------------------------
First-pass linkage of lab report results to Westmoreland Landfill 26R sites.

Linkage logic:
  Path A — f26r_location match (same-file): lab row's f26r_location matches a
            Westmoreland waste_location in the same PDF. Highest confidence.
  Path B — page proximity (same-file): no f26r_location match, but original_page
            > a Westmoreland form page in the same file. Linked to the nearest
            preceding form. Lower confidence.
  Path C — project_name match (cross-file): for lab rows in files NOT in the
            Westmoreland set, check whether project_name contains a Westmoreland
            site name as a word-bounded substring (case-insensitive).
            Names shorter than 5 chars are flagged needs_review=True.

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
# Path C: project_name substring match for files outside the overlap set
# ---------------------------------------------------------------------------

# Names shorter than this get a needs_review flag (more likely false positives)
_PATH_C_REVIEW_LEN = 5

def _build_site_patterns(sites):
    """Return list of (site_name, compiled_regex) with word-boundary matching."""
    patterns = []
    for site in sites:
        if not site or len(site) < 4:
            continue
        escaped = re.escape(site)
        pat = re.compile(r'(?<![A-Za-z])' + escaped + r'(?![A-Za-z])', re.I)
        patterns.append((site, pat))
    return patterns


def link_by_project_name(matches_df, lab_df, already_linked_files):
    """
    Path C: search project_name in lab rows NOT from already-linked files.
    Returns rows with site, waste_location, waste_code_26r, match_path,
    needs_review columns appended.
    """
    # Canonical site → waste_location mapping (pick first waste_location per site)
    _WELLPAD_SUFFIX2 = re.compile(r'\s*[-–]?\s*well\s*pad\b.*$', re.I)
    def _canon(n):
        if pd.isna(n): return ''
        n = str(n).strip()
        n = re.sub(r'\s{2,}.*$', '', n)
        n = _WELLPAD_SUFFIX2.sub('', n).strip()
        return re.sub(r'\s+', ' ', n)

    # Build site → representative waste_location and waste_code
    site_meta = {}
    for _, row in matches_df.iterrows():
        site = _canon(row['waste_location'])
        if site and site not in site_meta:
            site_meta[site] = {
                'waste_location': row['waste_location'],
                'waste_code_26r': row.get('waste_code', None),
            }

    patterns = _build_site_patterns(list(site_meta.keys()))

    # Restrict to lab rows outside the overlap set
    lab_outside = lab_df[~lab_df['original_filename'].isin(already_linked_files)].copy()
    print(f"  Lab rows in non-overlap files: {len(lab_outside):,}")

    # Build a project_name → matched site lookup (one pass over unique project_names)
    proj_to_site = {}
    proj_names = lab_outside['project_name'].dropna().unique()
    for proj in proj_names:
        for site, pat in patterns:
            if pat.search(proj):
                proj_to_site[proj] = site
                break   # take first (longest names are listed first if sorted desc)

    matched_proj = set(proj_to_site.keys())
    print(f"  Unique project_names matched: {len(matched_proj)}")

    hits = lab_outside[lab_outside['project_name'].isin(matched_proj)].copy()
    hits['site']          = hits['project_name'].map(proj_to_site)
    hits['waste_location'] = hits['site'].map(lambda s: site_meta.get(s, {}).get('waste_location', s))
    hits['waste_code_26r'] = hits['site'].map(lambda s: site_meta.get(s, {}).get('waste_code_26r', None))
    hits['match_path']    = 'project_name'
    hits['needs_review']  = hits['site'].map(lambda s: len(s) < _PATH_C_REVIEW_LEN)
    hits['page_number']   = None

    return hits


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
        nr_col = 'needs_review' if 'needs_review' in g.columns else None
        return pd.Series({
            'n_results':    len(g),
            'n_detect':     (g['result_flag'] != '<').sum(),
            'min':          g['result_value'].min(),
            'median':       g['result_value'].median(),
            'max':          g['result_value'].max(),
            'units':        first_mode(g['units']),
            'lab_names':    ', '.join(sorted(g['lab_name'].dropna().unique())),
            'match_paths':  ', '.join(sorted(g['match_path'].unique())),
            'needs_review': bool(g[nr_col].any()) if nr_col else False,
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
        maxBytes=0,
    )

    title = """
    <div style="padding:12px 0;">
      <h2 style="margin-bottom:4px;">Lab Results — Westmoreland Landfill Sites</h2>
      <p style="margin:0; color:#555;">
        One row per site × analyte. Results linked via: Path A (f26r_location match,
        same PDF), Path B (page proximity, same PDF), or Path C (project_name substring
        match, cross-file). needs_review=True flags short site names prone to false
        positives. Only numeric results shown; &lt;MDL detections counted separately.
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

    print("\nPath A+B: linking same-file lab results...")
    linked_ab = link_lab_to_26r(matches, lab)
    linked_ab['needs_review'] = False

    overlap_files = set(matches['filename'].unique()) & set(lab['original_filename'].unique())
    print(f"\nPath C: project_name matching for non-overlap files...")
    linked_c = link_by_project_name(matches, lab, overlap_files)

    linked = pd.concat([linked_ab, linked_c], ignore_index=True)
    print(f"\nTotal linked rows: {len(linked):,}")

    # Coverage report
    path_counts = linked['match_path'].value_counts()
    print(f"  Path A (f26r_location match):  {path_counts.get('f26r_location', 0):,}")
    print(f"  Path B (page proximity):        {path_counts.get('page_proximity', 0):,}")
    print(f"  Path C (project_name match):    {path_counts.get('project_name', 0):,}")
    print(f"    of which needs_review:         {linked[linked['match_path']=='project_name']['needs_review'].sum():,}")

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
