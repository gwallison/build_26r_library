import streamlit as st
import sqlite3
import pandas as pd
import os
from rapidfuzz import process, fuzz
import re

# --- Configuration ---
DB_PATH = "data/corpus/lab_report_corpus.db"
RESULTS_PATH = "data/output/batch_harvest_surgical_v2/results_v2.parquet"
ST_TITLE = "Lab Report Search Studio"
ST_ICON = "🧪"

st.set_page_config(page_title=ST_TITLE, page_icon=ST_ICON, layout="wide")

# --- Functions ---

@st.cache_data
def load_extracted_results():
    if not os.path.exists(RESULTS_PATH):
        return None
    try:
        return pd.read_parquet(RESULTS_PATH)
    except Exception as e:
        st.warning(f"Error loading extracted results: {e}")
        return None

results_df = load_extracted_results()

def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def run_query(query, fuzzy=False, lab_filter="All", min_score=0, limit=100):
    conn = get_connection()
    table = "pages_fuzzy" if fuzzy else "pages_idx"
    
    # base SQL query - text is now column 7, snippet uses 7
    sql = f"""
    SELECT filename, set_name, page_number, detected_lab, triage_score, 
           lab_start_page, lab_end_page,
           snippet({table}, 7, '***', '***', '...', 20) as snippet
    FROM {table}
    WHERE text MATCH ?
    """
    params = [query]
    
    if lab_filter != "All":
        sql += " AND detected_lab = ?"
        params.append(lab_filter)
        
    sql += " AND triage_score >= ?"
    params.append(min_score)
    
    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)
    
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df, None
    except sqlite3.OperationalError as e:
        return None, str(e)
    finally:
        conn.close()

def get_records_without_query(lab_filter="All", min_score=0, limit=100):
    conn = get_connection()
    sql = """
    SELECT filename, set_name, page_number, detected_lab, triage_score, 
           lab_start_page, lab_end_page,
           substr(text, 1, 150) as snippet
    FROM pages_idx
    """
    params = []
    conditions = []
    
    if lab_filter != "All":
        conditions.append("detected_lab = ?")
        params.append(lab_filter)
    conditions.append("triage_score >= ?")
    params.append(min_score)
    
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
        
    sql += " ORDER BY triage_score DESC LIMIT ?"
    params.append(limit)
    
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        df['snippet'] = df['snippet'].str.replace('\n', ' ') + '...'
        return df, None
    except sqlite3.OperationalError as e:
        return None, str(e)
    finally:
        conn.close()

@st.cache_data
def get_unique_labs():
    if not os.path.exists(DB_PATH):
        return ["All"]
    conn = get_connection()
    try:
        labs = [r[0] for r in conn.execute("SELECT DISTINCT detected_lab FROM pages_idx WHERE detected_lab IS NOT NULL ORDER BY detected_lab").fetchall()]
        return ["All"] + labs
    except:
        return ["All"]
    finally:
        conn.close()

@st.cache_data
def get_vocab():
    if not os.path.exists(DB_PATH):
        return []
    conn = get_connection()
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS temp.vocab USING fts5vocab('main', 'pages_idx', 'row')")
        words = [r[0] for r in conn.execute("SELECT term FROM temp.vocab WHERE term GLOB '[a-zA-Z]*'").fetchall() if len(r[0]) >= 3]
        return words
    except:
        return []
    finally:
        conn.close()

def get_variants(term, words, score_cutoff=80):
    if not term: return []
    matches = process.extract(term.lower(), words, scorer=fuzz.ratio, limit=10, score_cutoff=score_cutoff)
    return [{"variant": m[0], "score": round(m[1], 1)} for m in matches]

# --- UI Layout ---

st.title(f"{ST_ICON} {ST_TITLE}")

if not os.path.exists(DB_PATH):
    st.error(f"Database not found at `{DB_PATH}`. Please run `segment_lab_reports.py` and `build_lab_index.py` first.")
    st.stop()

# --- Sidebar Filters ---
unique_labs = get_unique_labs()

with st.sidebar:
    st.header("Search Filters")
    lab_filter = st.selectbox("Detected Lab Company:", options=unique_labs, index=0)
    min_score = st.slider("Minimum Triage Score:", min_value=5, max_value=50, value=5, step=1)
    
    st.divider()
    st.subheader("Search Options")
    is_fuzzy = st.checkbox("Fuzzy (Trigram) Index", help="Use for resilient matching on OCR typos")
    limit = st.number_input("Result Limit", min_value=10, max_value=2000, value=100, step=50)
    
    st.divider()
    st.subheader("Vocabulary Variant Finder")
    v_term = st.text_input("Find OCR variants of a term:", placeholder="e.g. Benzene")
    if v_term:
        vocab = get_vocab()
        vars = get_variants(v_term, vocab)
        if vars:
            for v in vars:
                st.code(f"{v['variant']} (Score: {v['score']})")
        else:
            st.write("No variants found.")

    st.divider()
    st.markdown("""
    **Query Tips:**
    - `Benzene AND Toluene`
    - `NEAR(Barium Radium, 10)`
    - `"Microbac Laboratories"`
    - `Methanol*` (Prefix wildcard)
    """)

# --- Main Search Interface ---
query = st.text_input("Enter search query (leave blank to browse filtered pages):", placeholder="e.g. Radium OR Chloride", help="FTS5 Full Text Search syntax supported")

df = None
error = None

if query.strip():
    df, error = run_query(query, fuzzy=is_fuzzy, lab_filter=lab_filter, min_score=min_score, limit=limit)
else:
    df, error = get_records_without_query(lab_filter=lab_filter, min_score=min_score, limit=limit)

if error:
    st.error(f"**Search Error:** {error}")
    st.info("Tip: Double check FTS5 operator casing, e.g. uppercase `AND`, `OR`, or `NEAR(...)`.")
elif df is not None:
    st.success(f"Found {len(df)} matching pages")
    
    # Format snippet to display clean text in table
    df['snippet_clean'] = df['snippet'].str.replace('***', '', regex=False)
    
    selection = st.dataframe(
        df[['filename', 'set_name', 'page_number', 'detected_lab', 'triage_score', 'snippet_clean']], 
        column_config={
            "filename": st.column_config.TextColumn("Filename", width="medium"),
            "set_name": st.column_config.TextColumn("Set", width="small"),
            "page_number": st.column_config.NumberColumn("Pg", width="small"),
            "detected_lab": st.column_config.TextColumn("Detected Lab", width="small"),
            "triage_score": st.column_config.NumberColumn("Score", width="small"),
            "snippet_clean": st.column_config.TextColumn("Snippet (Context)", width="large"),
        },
        hide_index=True,
        use_container_width=True,
        on_select="rerun",
        selection_mode="single-row"
    )
    
    if selection and selection.selection.rows:
        selected_row_idx = selection.selection.rows[0]
        selected_row = df.iloc[selected_row_idx]
        
        st.divider()
        st.subheader(f"Inspector: {selected_row['filename']} | Page {selected_row['page_number']} | {selected_row['detected_lab']}")
        
        # PDF Link
        pdf_url = f"https://storage.googleapis.com/fta-form26r-library/full-set/{selected_row['set_name']}/{selected_row['filename'].replace(' ', '%20')}#page={selected_row['page_number']}"
        st.link_button("📂 Open Original PDF in New Tab", pdf_url)
        
        # --- Extracted Gemini Results Section ---
        if results_df is not None:
            # Filter results for this PDF file
            file_results = results_df[
                (results_df['original_filename'] == selected_row['filename']) &
                (results_df['set_name'] == selected_row['set_name'])
            ]
            
            if not file_results.empty:
                st.write("")
                st.markdown("### 🧪 Extracted Chemical Analytes (Gemini)")
                
                # Filter widgets side-by-side
                col_filt1, col_filt2 = st.columns(2)
                with col_filt1:
                    unique_analytes = sorted(file_results['analyte'].dropna().astype(str).unique().tolist())
                    selected_analytes = st.multiselect("Filter by Analyte:", options=unique_analytes)
                with col_filt2:
                    unique_matrices = sorted(file_results['matrix'].dropna().astype(str).unique().tolist())
                    selected_matrices = st.multiselect("Filter by Matrix:", options=unique_matrices)
                
                # Base page and consecutive page frames before filters
                page_results = file_results[file_results['original_page'] == int(selected_row['page_number'])]
                
                # Get pre-calculated consecutive page range from row metadata
                start_page = selected_row.get('lab_start_page')
                end_page = selected_row.get('lab_end_page')
                if start_page is not None and end_page is not None and not pd.isna(start_page) and not pd.isna(end_page):
                    consecutive_pages = list(range(int(start_page), int(end_page) + 1))
                else:
                    consecutive_pages = [int(selected_row['page_number'])]
                
                consec_results = file_results[file_results['original_page'].isin(consecutive_pages)]
                
                # Apply filters to all dataframes
                if selected_analytes:
                    page_results = page_results[page_results['analyte'].isin(selected_analytes)]
                    consec_results = consec_results[consec_results['analyte'].isin(selected_analytes)]
                    file_results = file_results[file_results['analyte'].isin(selected_analytes)]
                if selected_matrices:
                    page_results = page_results[page_results['matrix'].isin(selected_matrices)]
                    consec_results = consec_results[consec_results['matrix'].isin(selected_matrices)]
                    file_results = file_results[file_results['matrix'].isin(selected_matrices)]
                
                # Create three tabs
                tab_page, tab_consec, tab_file = st.tabs([
                    f"📄 Selected Page ({len(page_results)} analytes)", 
                    f"🔗 Consecutive Pages {min(consecutive_pages)}-{max(consecutive_pages)} ({len(consec_results)} analytes)",
                    f"📂 Entire PDF File ({len(file_results)} analytes)"
                ])
                
                with tab_page:
                    if not page_results.empty:
                        st.dataframe(
                            page_results[['lab_sample_id', 'analyte', 'result', 'units', 'reporting_limit', 'mdl', 'collection_date', 'matrix']],
                            column_config={
                                "lab_sample_id": "Lab Sample ID",
                                "analyte": "Analyte",
                                "result": "Result",
                                "units": "Units",
                                "reporting_limit": "Rep. Limit",
                                "mdl": "MDL",
                                "collection_date": "Coll. Date",
                                "matrix": "Matrix"
                            },
                            hide_index=True,
                            use_container_width=True
                        )
                        page_csv = page_results.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Selected Page Analytes as CSV",
                            data=page_csv,
                            file_name=f"page_{selected_row['page_number']}_analytes.csv",
                            mime='text/csv',
                            key="dl_page"
                        )
                    else:
                        st.info("No analytes extracted from this specific page by Gemini (or page was not sent to LLM).")
                        
                with tab_consec:
                    if not consec_results.empty:
                        st.dataframe(
                            consec_results[['original_page', 'lab_sample_id', 'analyte', 'result', 'units', 'reporting_limit', 'mdl', 'collection_date', 'matrix']].sort_values(by='original_page'),
                            column_config={
                                "original_page": "Pg",
                                "lab_sample_id": "Lab Sample ID",
                                "analyte": "Analyte",
                                "result": "Result",
                                "units": "Units",
                                "reporting_limit": "Rep. Limit",
                                "mdl": "MDL",
                                "collection_date": "Coll. Date",
                                "matrix": "Matrix"
                            },
                            hide_index=True,
                            use_container_width=True
                        )
                        consec_csv = consec_results.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Consecutive Pages Analytes as CSV",
                            data=consec_csv,
                            file_name=f"consecutive_pages_{min(consecutive_pages)}_{max(consecutive_pages)}_analytes.csv",
                            mime='text/csv',
                            key="dl_consec"
                        )
                    else:
                        st.info("No analytes extracted from these consecutive pages by Gemini.")
                        
                with tab_file:
                    if not file_results.empty:
                        st.dataframe(
                            file_results[['original_page', 'lab_sample_id', 'analyte', 'result', 'units', 'reporting_limit', 'mdl', 'collection_date', 'matrix']].sort_values(by='original_page'),
                            column_config={
                                "original_page": "Pg",
                                "lab_sample_id": "Lab Sample ID",
                                "analyte": "Analyte",
                                "result": "Result",
                                "units": "Units",
                                "reporting_limit": "Rep. Limit",
                                "mdl": "MDL",
                                "collection_date": "Coll. Date",
                                "matrix": "Matrix"
                            },
                            hide_index=True,
                            use_container_width=True
                        )
                        file_csv = file_results.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Entire PDF Analytes as CSV",
                            data=file_csv,
                            file_name=f"pdf_{selected_row['filename']}_analytes.csv",
                            mime='text/csv',
                            key="dl_file"
                        )
                    else:
                        st.info("No analytes found matching your current filter criteria.")
            else:
                st.info("No Gemini extraction results found for this PDF in the results database.")
        else:
            st.warning("Extracted results database (results_v2.parquet) not found at 'data/output/batch_harvest_surgical_v2/results_v2.parquet'.")
            
    # Download results section
    st.divider()
    col_dl1, col_dl2 = st.columns(2)
    
    with col_dl1:
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Table Results as CSV",
            data=csv,
            file_name="lab_search_results.csv",
            mime='text/csv',
            use_container_width=True,
            key="dl_table_csv"
        )
        
    with col_dl2:
        if st.button("📦 Build & Zip Clipped Lab Report PDFs", use_container_width=True, key="btn_zip_pdfs"):
            if df.empty:
                st.warning("No search results found to clip.")
            else:
                import tempfile
                import zipfile
                import shutil
                import fitz
                
                # Find unique lab report segments
                unique_reports = df[['filename', 'set_name', 'lab_start_page', 'lab_end_page']].drop_duplicates()
                unique_reports = unique_reports.dropna(subset=['lab_start_page', 'lab_end_page'])
                
                if unique_reports.empty:
                    st.warning("No valid lab report segments found in search results to clip.")
                else:
                    # Setup temp dir
                    temp_dir = tempfile.mkdtemp()
                    zip_path = os.path.join(temp_dir, "clipped_lab_reports.zip")
                    
                    PDF_LIBRARY_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
                    
                    clipped_count = 0
                    missing_files = []
                    
                    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        for _, row in unique_reports.iterrows():
                            fn = row['filename']
                            sn = row['set_name']
                            start = int(row['lab_start_page'])
                            end = int(row['lab_end_page'])
                            
                            pdf_path = os.path.join(PDF_LIBRARY_ROOT, sn, fn)
                            if not os.path.exists(pdf_path):
                                pdf_path = os.path.join(PDF_LIBRARY_ROOT, fn)
                                
                            if not os.path.exists(pdf_path):
                                missing_files.append(fn)
                                continue
                                
                            try:
                                doc = fitz.open(pdf_path)
                                new_doc = fitz.open()
                                
                                # fitz is 0-based, pages is 1-based
                                for p_idx in range(start - 1, end):
                                    if 0 <= p_idx < len(doc):
                                        new_doc.insert_pdf(doc, from_page=p_idx, to_page=p_idx)
                                        
                                clip_name = f"{os.path.splitext(fn)[0]}_pages_{start}_{end}.pdf"
                                clip_temp_path = os.path.join(temp_dir, clip_name)
                                new_doc.save(clip_temp_path)
                                new_doc.close()
                                doc.close()
                                
                                # Write to zip
                                zip_file.write(clip_temp_path, clip_name)
                                clipped_count += 1
                            except Exception as ex:
                                st.warning(f"Error clipping {fn}: {ex}")
                                
                    if clipped_count > 0:
                        st.success(f"Successfully clipped {clipped_count} laboratory reports!")
                        if missing_files:
                            st.warning(f"Could not find {len(set(missing_files))} original PDFs on disk.")
                            
                        # Read the zip into memory for download_button
                        with open(zip_path, "rb") as zf:
                            zip_bytes = zf.read()
                            
                        st.download_button(
                            label="📥 Download Clipped PDFs Zip Archive",
                            data=zip_bytes,
                            file_name="clipped_lab_reports.zip",
                            mime="application/zip",
                            use_container_width=True,
                            key="download_clipped_zip"
                        )
                    else:
                        st.error("No PDFs could be successfully clipped. Please verify that original PDFs exist in D:\\PA_Form26r_PDFs\\all_pdfs")
                        
                    # Clean up temp dir
                    shutil.rmtree(temp_dir)
