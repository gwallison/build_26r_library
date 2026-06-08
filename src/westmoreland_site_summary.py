# -*- coding: utf-8 -*-
"""
westmoreland_site_summary.py
-----------------------------
Produces a site-level summary table of well pads that sent waste to the
Westmoreland Waste LLC Sanitary Landfill, as recorded in PA Form 26R filings.

Input:
    G:/My Drive/sandbox/26R/data_cleanup_26R/processed/westmoreland_landfill_f26r_matches.csv

Outputs:
    data/output/westmoreland_sites.csv   — machine-readable site summary
    data/output/westmoreland_sites.html  — interactive HTML table
"""

import os
import re
import pandas as pd
import itables
from itables import init_notebook_mode

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, 'data', 'output')

INPUT_CSV  = r"G:\My Drive\sandbox\26R\data_cleanup_26R\processed\westmoreland_landfill_f26r_matches.csv"
OUT_CSV    = os.path.join(OUTPUT_DIR, 'westmoreland_sites.csv')
OUT_HTML   = os.path.join(OUTPUT_DIR, 'westmoreland_sites.html')

# ---------------------------------------------------------------------------
# Canonical waste code descriptions
# (most informative description per code, chosen manually from the data)
# ---------------------------------------------------------------------------

WASTE_CODE_LABELS = {
    418: "Sawdust / Wood Shavings",
    506: "Contaminated Soil (Non-Petroleum)",
    507: "Contaminated Soil (Petroleum)",
    508: "Virgin Petroleum Material Contaminated",
    710: "Plant Trash / General Plant Refuse",
    802: "Brine / Produced Fluid",
    804: "Wastewater Treatment Sludge",
    805: "Fracing Fluid Waste",
    806: "Synthetic Liner Materials",
    810: "Drill Cuttings",
    811: "Soil Contaminated by O&G Spills",
    814: "Soil Contaminated by O&G Related",
    899: "Other Oil and Gas Wastes",
}

# ---------------------------------------------------------------------------
# Site name normalization
# ---------------------------------------------------------------------------

_WELLPAD_SUFFIX = re.compile(
    r'\s*[-–]?\s*well\s*pad\b.*$',   # strips " - Well Pad", " Well Pad", " - Well Pad / Impoundment"
    re.I
)

def canonical_site(name):
    if pd.isna(name):
        return ''
    name = str(name).strip()
    # Strip OCR-garbled long tails (address, email, etc. bleed-in)
    name = re.sub(r'\s{2,}.*$', '', name)          # anything after 2+ spaces is noise
    # Strip "Well Pad" suffix variants
    name = _WELLPAD_SUFFIX.sub('', name).strip()
    # Collapse internal whitespace
    name = re.sub(r'\s+', ' ', name)
    return name

# ---------------------------------------------------------------------------
# Build site table
# ---------------------------------------------------------------------------

def build_site_table(df):
    df = df.copy()

    # Normalize site name
    df['site'] = df['waste_location'].map(canonical_site)

    # Normalize waste code to int (drop nulls)
    df['code_int'] = pd.to_numeric(df['waste_code'], errors='coerce').dropna().astype(int)

    # Canonical description per code
    df['code_label'] = df['code_int'].map(WASTE_CODE_LABELS)

    def agg_site(g):
        # Dates: unique non-null, sorted
        dates = sorted(g['date_prepared'].dropna().unique())

        # Company: use most frequent
        company = g['company_name'].mode().iloc[0] if len(g) > 0 else ''

        # Waste codes: unique ints, sorted; labels from canonical map
        codes = sorted(g['code_int'].dropna().unique().astype(int))
        code_str  = ', '.join(str(c) for c in codes)
        label_str = ', '.join(
            WASTE_CODE_LABELS.get(c, str(c)) for c in codes
        )

        # Unique form filings
        n_forms = g['filename'].nunique()

        # Earliest and latest date
        date_str  = ', '.join(dates) if dates else '(not captured)'

        return pd.Series({
            'company':        company,
            'dates':          date_str,
            'n_forms':        n_forms,
            'waste_codes':    code_str,
            'waste_types':    label_str,
        })

    site_df = (
        df[df['site'] != '']
        .groupby('site', sort=True)
        .apply(agg_site, include_groups=False)
        .reset_index()
    )

    return site_df


# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

table_styling = """<style>
  body {
    font-family: "Noto Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu,
                 "Helvetica Neue", sans-serif;
    font-size: 15px;
    color: rgb(9, 66, 100);
  }
  table, th, td { border-color: rgb(214, 239, 238); }
  th { font-weight: 500; }
</style>"""


def make_html(site_df, out_path):
    init_notebook_mode(all_interactive=True, connected=True)

    html = itables.to_html_datatable(
        site_df.reset_index(drop=True),
        connected=True,
        pageLength=25,
        display_logo_when_loading=False,
        lengthMenu=[10, 25, 50, 100],
        buttons=['pageLength', 'copyHtml5', 'csvHtml5', 'colvis'],
        column_filters="footer",
        footer=True,
    )

    title = """
    <div style="padding: 12px 0;">
      <h2 style="margin-bottom:4px;">Sites Sending Waste to Westmoreland Waste LLC Sanitary Landfill</h2>
      <p style="margin:0; color:#555;">
        Each row is a unique waste generation site (well pad or facility) identified in PA Form 26R filings.
        Site names are normalized — minor spelling variants across annual reports are merged.
        Dates shown are form preparation dates; null dates indicate filings where the date field was not captured.
      </p>
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
    print(f"Reading {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV)
    print(f"  {len(df)} rows, {df['waste_location'].nunique()} unique raw site names")

    site_df = build_site_table(df)
    print(f"  {len(site_df)} canonical sites after normalization")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    site_df.to_csv(OUT_CSV, index=False)
    print(f"CSV written to {OUT_CSV}")

    make_html(site_df, OUT_HTML)

    print("\nSite table preview:")
    print(site_df.to_string(index=False))
