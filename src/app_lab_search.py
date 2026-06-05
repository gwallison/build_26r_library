import streamlit as st
import sqlite3
import pandas as pd
import os
from rapidfuzz import process, fuzz
import re

# --- Configuration ---
DB_PATH = "data/corpus/lab_report_corpus.db"
ST_TITLE = "Lab Report Search Studio"
ST_ICON = "🧪"

st.set_page_config(page_title=ST_TITLE, page_icon=ST_ICON, layout="wide")

# --- Functions ---

def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def run_query(query, fuzzy=False, lab_filter="All", min_score=0, limit=100):
    conn = get_connection()
    table = "pages_fuzzy" if fuzzy else "pages_idx"
    
    # base SQL query
    sql = f"""
    SELECT filename, set_name, page_number, detected_lab, triage_score, 
           snippet({table}, 5, '***', '***', '...', 20) as snippet
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
        
        # Fetch full text
        conn = get_connection()
        table = "pages_fuzzy" if is_fuzzy else "pages_idx"
        full_text_sql = f"SELECT text FROM {table} WHERE filename = ? AND page_number = ?"
        full_text = conn.execute(full_text_sql, (selected_row['filename'], selected_row['page_number'])).fetchone()[0]
        conn.close()
        
        with st.expander("Show Full Page Text", expanded=True):
            display_text = full_text
            if query.strip():
                # Highlight terms in the full page display
                search_terms = [t.strip('"') for t in query.split() if len(t) > 2 and t.upper() not in ['AND', 'OR', 'NOT', 'NEAR']]
                for term in search_terms:
                    display_text = re.sub(f"({re.escape(term)})", r"**\1**", display_text, flags=re.IGNORECASE)
            st.markdown(display_text)
            
    # Download results button
    st.divider()
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Results as CSV",
        data=csv,
        file_name=f"lab_search_results.csv",
        mime='text/csv',
    )
