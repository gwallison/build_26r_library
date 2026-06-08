# -*- coding: utf-8 -*-
"""
westmoreland_map.py
-------------------
Produces an interactive Folium map of well pad sites that sent waste to the
Westmoreland Waste LLC Sanitary Landfill, as recorded in PA Form 26R filings.

Inputs:
    data/output/westmoreland_sites.csv         (site summary from westmoreland_site_summary.py)
    data/output/all_harvested_form26r_v3_fer.parquet  (lat/lon from FER registry)
    data_cleanup_26R/processed/westmoreland_landfill_f26r_matches.csv

Output:
    data/output/westmoreland_map.html
"""

import os
import re
import pandas as pd
import folium
from folium.plugins import MarkerCluster

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, 'data', 'output')

MATCHES_CSV  = r"G:\My Drive\sandbox\26R\data_cleanup_26R\processed\westmoreland_landfill_f26r_matches.csv"
SITES_CSV    = os.path.join(OUTPUT_DIR, 'westmoreland_sites.csv')
FER_PARQUET  = os.path.join(OUTPUT_DIR, 'all_harvested_form26r_v3_fer.parquet')
OUT_HTML     = os.path.join(OUTPUT_DIR, 'westmoreland_map.html')

# Westmoreland Waste LLC Sanitary Landfill — Belle Vernon, Westmoreland County
LANDFILL_LAT = 40.1275
LANDFILL_LON = -79.8540
LANDFILL_NAME = "Westmoreland Waste LLC Sanitary Landfill"
LANDFILL_ADDR = "111 Conner Lane, Belle Vernon, PA 15012"

# ---------------------------------------------------------------------------
# Site name normalization (same as site summary script)
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
# Company colour palette
# ---------------------------------------------------------------------------

COMPANY_COLORS = {
    'EQT Production Company':                        '#1f77b4',   # blue
    'RE, LLC - Previously Rice Energy':              '#ff7f0e',   # orange
    'Greylock Production, LLC':                      '#2ca02c',   # green
    'XPR Resources LLC':                             '#9467bd',   # purple
    'Huntley and Huntley Energy Exploration, LLC':   '#d62728',   # red
    'Campbell Oil & Gas, inc':                       '#8c564b',   # brown
}
DEFAULT_COLOR = '#7f7f7f'

# ---------------------------------------------------------------------------
# Build enriched site geo table
# ---------------------------------------------------------------------------

def build_geo_table(matches_csv, sites_csv, fer_parquet):
    matches = pd.read_csv(matches_csv)
    sites   = pd.read_csv(sites_csv)

    # Attach canonical site name to matches
    matches['site'] = matches['waste_location'].map(canonical_site)

    # Join FER lat/lon (from v3_fer) onto matches rows
    fer_cols = ['filename', 'page_number',
                'WELL_PAD_LATITUDE', 'WELL_PAD_LONGITUDE',
                'COUNTY', 'MUNICIPALITY', 'anchor_confidence', 'anchor_method']
    fer = pd.read_parquet(fer_parquet, columns=fer_cols).drop_duplicates()
    matches = matches.merge(fer, on=['filename', 'page_number'], how='left')

    # One coordinate per canonical site — prefer pad_exact, then pad_idf
    conf_rank = {'pad_exact': 0, 'pad_idf': 1, 'coord_only': 2}
    matches['_rank'] = matches['anchor_method'].map(conf_rank).fillna(9)
    site_geo = (
        matches[matches['WELL_PAD_LATITUDE'].notna()]
        .sort_values('_rank')
        .drop_duplicates('site')
        [['site', 'WELL_PAD_LATITUDE', 'WELL_PAD_LONGITUDE',
          'COUNTY', 'MUNICIPALITY', 'anchor_confidence', 'anchor_method']]
    )

    # Merge with site summary (company, dates, waste codes)
    geo = site_geo.merge(sites, on='site', how='left')
    return geo


# ---------------------------------------------------------------------------
# Build map
# ---------------------------------------------------------------------------

def build_map(geo):
    # Centre on mean of site coordinates
    centre_lat = geo['WELL_PAD_LATITUDE'].mean()
    centre_lon = geo['WELL_PAD_LONGITUDE'].mean()

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=9,
        tiles='CartoDB positron',
    )

    # --- Landfill marker ---
    landfill_icon = folium.Icon(color='red', icon='industry', prefix='fa')
    folium.Marker(
        location=[LANDFILL_LAT, LANDFILL_LON],
        tooltip=LANDFILL_NAME,
        popup=folium.Popup(
            f"<b>{LANDFILL_NAME}</b><br>{LANDFILL_ADDR}<br>"
            f"<i>Receiving facility for all sites shown</i>",
            max_width=280,
        ),
        icon=landfill_icon,
    ).add_to(m)

    # --- Site markers ---
    for _, row in geo.iterrows():
        color = COMPANY_COLORS.get(row.get('company', ''), DEFAULT_COLOR)
        dates_str   = row.get('dates', '(not captured)')
        codes_str   = row.get('waste_codes', '')
        types_str   = row.get('waste_types', '')
        county_str  = row.get('COUNTY', '')
        muni_str    = row.get('MUNICIPALITY', '')
        conf_str    = f"{row.get('anchor_confidence', ''):.2f}" if pd.notna(row.get('anchor_confidence')) else ''

        popup_html = f"""
        <div style="font-family:sans-serif; font-size:13px; min-width:220px">
          <b style="font-size:14px">{row['site']}</b><br>
          <span style="color:#555">{row.get('company','')}</span><br>
          <hr style="margin:4px 0">
          <b>Location:</b> {muni_str}, {county_str} Co.<br>
          <b>26R dates:</b> {dates_str}<br>
          <b>Waste codes:</b> {codes_str}<br>
          <span style="color:#666; font-size:11px">{types_str}</span><br>
          <span style="color:#aaa; font-size:10px">FER anchor: {row.get('anchor_method','')} (conf {conf_str})</span>
        </div>
        """

        folium.CircleMarker(
            location=[row['WELL_PAD_LATITUDE'], row['WELL_PAD_LONGITUDE']],
            radius=7,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            weight=1.5,
            tooltip=row['site'],
            popup=folium.Popup(popup_html, max_width=300),
        ).add_to(m)

    # --- Legend ---
    legend_items = [
        (LANDFILL_NAME, '#e74c3c', '★'),
    ]
    represented = geo['company'].dropna().unique()
    for company, color in COMPANY_COLORS.items():
        if company in represented:
            legend_items.append((company, color, '●'))

    legend_html = """
    <div style="
        position: fixed; bottom: 30px; left: 30px; z-index: 1000;
        background: white; padding: 12px 16px; border-radius: 6px;
        border: 1px solid #ccc; font-family: sans-serif; font-size: 13px;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.15);
    ">
    <b style="font-size:14px">Sites → Westmoreland Landfill</b><br>
    <span style="color:#888; font-size:11px">PA Form 26R filings</span>
    <hr style="margin:6px 0">
    """
    for label, color, symbol in legend_items:
        legend_html += (
            f'<span style="color:{color}; font-size:16px">{symbol}</span>'
            f'&nbsp;{label}<br>'
        )
    legend_html += "</div>"

    m.get_root().html.add_child(folium.Element(legend_html))

    return m


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Building site geo table...")
    geo = build_geo_table(MATCHES_CSV, SITES_CSV, FER_PARQUET)
    print(f"  {len(geo)} sites with coordinates (of 96 canonical sites)")

    print("Building map...")
    m = build_map(geo)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    m.save(OUT_HTML)
    print(f"Map written to {OUT_HTML}")

    print("\nSite breakdown by company:")
    print(geo['company'].value_counts().to_string())
    print("\nSite breakdown by county:")
    print(geo['COUNTY'].value_counts().to_string())
