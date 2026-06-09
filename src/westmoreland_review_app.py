# -*- coding: utf-8 -*-
"""
westmoreland_review_app.py
--------------------------
Streamlit app for evaluating whether lab reports are legitimately linked to
Westmoreland Landfill well-pad sites.

Run with:
    streamlit run src/westmoreland_review_app.py

Inputs (read-only):
    data/output/westmoreland_lab_links.parquet
    G:/.../westmoreland_landfill_f26r_matches.csv
"""

import os
import re
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Westmoreland Site Review",
    layout="wide",
    initial_sidebar_state="expanded",
)

PROJECT_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR        = os.path.join(PROJECT_ROOT, "data", "output")
LAB_LINKS_PARQUET = os.path.join(OUTPUT_DIR, "westmoreland_lab_links.parquet")
MATCHES_CSV       = r"G:\My Drive\sandbox\26R\data_cleanup_26R\processed\westmoreland_landfill_f26r_matches.csv"
GCS_BASE          = "https://storage.googleapis.com/fta-form26r-library/full-set/"

# ---------------------------------------------------------------------------
# Site name normalization (mirrors westmoreland_lab_link.py)
# ---------------------------------------------------------------------------

_WELLPAD_SUFFIX = re.compile(r"\s*[-–]?\s*well\s*pad\b.*$", re.I)

def canonical_site(name):
    if pd.isna(name):
        return ""
    name = str(name).strip()
    name = re.sub(r"\s{2,}.*$", "", name)
    name = _WELLPAD_SUFFIX.sub("", name).strip()
    return re.sub(r"\s+", " ", name)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(show_spinner="Loading data…")
def load_data():
    matches = pd.read_csv(MATCHES_CSV)
    matches["site"] = matches["waste_location"].map(canonical_site)

    lab = pd.read_parquet(LAB_LINKS_PARQUET)
    # Ensure needs_review exists and is bool
    if "needs_review" not in lab.columns:
        lab["needs_review"] = False
    lab["needs_review"] = lab["needs_review"].fillna(False).astype(bool)

    return matches, lab


matches_all, lab_all = load_data()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gcs_url(set_name, filename, page):
    fn = str(filename).replace(" ", "%20")
    return f"{GCS_BASE}{set_name}/{fn}#page={int(page)}"


MATCH_PATH_LABEL = {
    "f26r_location":  "A — f26r_location (same file, field match)",
    "page_proximity": "B — page proximity (same file, preceding form)",
    "project_name":   "C — project_name (cross-file, substring match)",
}

MATCH_PATH_COLOR = {
    "f26r_location":  "green",
    "page_proximity": "orange",
    "project_name":   "red",
}

# ---------------------------------------------------------------------------
# Sidebar — site selector
# ---------------------------------------------------------------------------

all_sites = sorted(lab_all["site"].dropna().unique())

st.sidebar.title("Westmoreland Site Review")
st.sidebar.caption(f"{len(all_sites)} sites with lab results")

selected = st.sidebar.selectbox("Select site", all_sites)

# Per-site data
site_lab     = lab_all[lab_all["site"] == selected].copy()
site_matches = matches_all[matches_all["site"] == selected].copy()

# Match path breakdown in sidebar
path_counts = site_lab["match_path"].value_counts()
st.sidebar.markdown("**Match path breakdown**")
for path in ["f26r_location", "page_proximity", "project_name"]:
    n = path_counts.get(path, 0)
    if n:
        label = path.replace("_", " ")
        color = MATCH_PATH_COLOR[path]
        st.sidebar.markdown(
            f"<span style='color:{color}'>●</span> &nbsp;{label}: **{n:,}**",
            unsafe_allow_html=True,
        )

# needs_review warning
if site_lab["needs_review"].any():
    st.sidebar.warning(
        "**needs_review** — site name is short (<5 chars). "
        "Check Path C links carefully for false positives."
    )

# ---------------------------------------------------------------------------
# Main — site header
# ---------------------------------------------------------------------------

company    = site_matches["company_name"].mode().iloc[0] if len(site_matches) > 0 else "—"
dates      = sorted(site_matches["date_prepared"].dropna().unique())
dates_str  = ", ".join(dates) if dates else "(not captured)"
waste_codes = ", ".join(sorted(site_matches["waste_code"].dropna().astype(str).unique()))

st.title(selected)
st.caption(f"{company}  ·  26R dates: {dates_str}  ·  Waste codes: {waste_codes}")

m1, m2, m3, m4 = st.columns(4)
m1.metric("26R filings",  len(site_matches))
m2.metric("Lab files",    site_lab["original_filename"].nunique())
m3.metric("Lab rows",     f"{len(site_lab):,}")
m4.metric("Analytes",     site_lab["analyte_norm"].nunique())

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_26r, tab_reports, tab_analytes = st.tabs(
    ["26R Forms", "Lab Reports (by file)", "Analyte Results"]
)

# ===========================================================================
# Tab 1 — 26R Forms
# ===========================================================================

with tab_26r:
    if site_matches.empty:
        st.info("No 26R filings found for this site.")
    else:
        st.subheader(f"{len(site_matches)} Form 26R filing rows")

        display = site_matches[[
            "date_prepared", "company_name",
            "waste_code", "waste_description",
            "waste_location",
            "volume_shipped", "volume_unit",
            "facility_name", "filename", "page_number", "url",
        ]].copy().rename(columns={"url": "pdf_link"})

        st.dataframe(
            display.reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
            column_config={
                "pdf_link": st.column_config.LinkColumn(
                    "PDF", display_text="Open ↗"
                ),
                "volume_shipped": st.column_config.NumberColumn(
                    "Volume", format="%.0f"
                ),
            },
        )

# ===========================================================================
# Tab 2 — Lab Reports grouped by file
# ===========================================================================

with tab_reports:
    if site_lab.empty:
        st.info("No lab results linked to this site.")
    else:
        st.subheader(
            f"{site_lab['original_filename'].nunique()} unique lab report files"
        )
        st.caption(
            "One row per lab PDF. "
            "Path A/B are from the same PDF as a 26R filing. "
            "Path C is a cross-file project_name match — verify carefully."
        )

        def summarise_file(g):
            row0 = g.iloc[0]
            return pd.Series({
                "match_path":       row0["match_path"],
                "needs_review":     bool(g["needs_review"].any()),
                "client_name":      row0["client_name"] or "—",
                "lab_name":         row0["lab_name"] or "—",
                "project_name":     row0["project_name"] or "—",
                "waste_location":   row0.get("waste_location", "—") or "—",
                "collection_dates": ", ".join(
                    sorted(g["collection_date"].dropna().unique())[:6]
                ),
                "n_analytes":       g["analyte_norm"].nunique(),
                "n_rows":           len(g),
                "pdf_link":         gcs_url(
                    row0["set_name"], row0["original_filename"], row0["original_page"]
                ),
            })

        report_df = (
            site_lab
            .groupby("original_filename", sort=False)
            .apply(summarise_file, include_groups=False)
            .reset_index()
            .rename(columns={"original_filename": "filename"})
        )

        path_order = {"f26r_location": 0, "page_proximity": 1, "project_name": 2}
        report_df["_sort"] = (
            report_df["needs_review"].astype(int) * 10
            + report_df["match_path"].map(path_order).fillna(9)
        )
        report_df = report_df.sort_values("_sort").drop(columns="_sort")

        st.dataframe(
            report_df[[
                "filename", "match_path", "needs_review",
                "client_name", "lab_name", "project_name",
                "waste_location", "collection_dates",
                "n_analytes", "n_rows", "pdf_link",
            ]].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
            column_config={
                "pdf_link": st.column_config.LinkColumn(
                    "PDF", display_text="Open ↗"
                ),
                "needs_review": st.column_config.CheckboxColumn("Review?"),
            },
            height=min(700, (len(report_df) + 2) * 36 + 38),
        )

# ===========================================================================
# Tab 3 — Full analyte results with filters
# ===========================================================================

with tab_analytes:
    if site_lab.empty:
        st.info("No lab results linked to this site.")
    else:
        st.subheader(f"{len(site_lab):,} analyte result rows")

        # --- Filters ---
        fc1, fc2, fc3 = st.columns([1, 1, 2])

        all_paths = sorted(site_lab["match_path"].unique())
        sel_paths = fc1.multiselect(
            "Match path", all_paths, default=all_paths, key="path_filter"
        )

        all_analytes = sorted(site_lab["analyte_norm"].dropna().unique())
        sel_analytes = fc2.multiselect(
            "Analyte (blank = all)", all_analytes, key="analyte_filter"
        )

        only_detects = fc3.checkbox("Detections only (exclude <MDL)", value=False)

        filtered = site_lab[site_lab["match_path"].isin(sel_paths)].copy()
        if sel_analytes:
            filtered = filtered[filtered["analyte_norm"].isin(sel_analytes)]
        if only_detects:
            filtered = filtered[filtered["result_flag"] != "<"]

        st.caption(f"Showing {len(filtered):,} rows after filters.")

        # Build PDF link per row
        filtered["pdf_link"] = filtered.apply(
            lambda r: gcs_url(r["set_name"], r["original_filename"], r["original_page"]),
            axis=1,
        )

        display_cols = [
            "match_path", "needs_review",
            "analyte_norm", "result_flag", "result_value", "units",
            "collection_date", "lab_name", "client_name", "project_name",
            "waste_location",
            "original_filename", "original_page", "pdf_link",
        ]

        st.dataframe(
            filtered[display_cols].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
            height=550,
            column_config={
                "pdf_link": st.column_config.LinkColumn(
                    "PDF", display_text="Open ↗"
                ),
                "needs_review": st.column_config.CheckboxColumn("Review?"),
                "result_value": st.column_config.NumberColumn(
                    "Result", format="%.4g"
                ),
            },
        )
