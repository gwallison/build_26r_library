import streamlit as st
import sqlite3
import pandas as pd
import os
from rapidfuzz import process, fuzz

# --- Configuration ---
DB_PATH = "data/corpus/corpus_search.db"
ST_TITLE = "Corpus Search Studio"
ST_ICON = "🔍"

st.set_page_config(page_title=ST_TITLE, page_icon=ST_ICON, layout="wide")

# --- Functions ---

def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def run_query(query, fuzzy=False, limit=100):
    conn = get_connection()
    table = "pages_fuzzy" if fuzzy else "pages_idx"
    
    # columns are: 0:filename, 1:set_name, 2:page_number, 3:text
    sql = f"""
    SELECT filename, set_name, page_number, snippet({table}, 3, '***', '***', '...', 20) as snippet
    FROM {table}
    WHERE text MATCH ?
    ORDER BY rank
    LIMIT ?
    """
    try:
        df = pd.read_sql_query(sql, conn, params=(query, limit))
        return df, None
    except sqlite3.OperationalError as e:
        return None, str(e)
    finally:
        conn.close()

@st.cache_data
def get_vocab():
    if not os.path.exists(DB_PATH):
        return []
    conn = get_connection()
    try:
        # Create vocab table if not exists (in temp)
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
    st.error(f"Database not found at `{DB_PATH}`. Please run index build scripts first.")
    st.stop()

# --- Sidebar: Tools & Help ---
with st.sidebar:
    st.header("Search Tools")
    is_fuzzy = st.checkbox("Fuzzy (Trigram) Index", help="Use this if you suspect OCR typos (e.g., 'Benzen3')")
    limit = st.number_input("Result Limit", min_value=10, max_value=1000, value=100, step=50)
    
    st.divider()
    st.subheader("Variant Finder")
    v_term = st.text_input("Test a term for OCR variants:", placeholder="e.g. Benzene")
    if v_term:
        vocab = get_vocab()
        vars = get_variants(v_term, vocab)
        if vars:
            for v in vars:
                st.code(v['variant'])
        else:
            st.write("No variants found.")

    st.divider()
    st.markdown("""
    **Query Tips:**
    - `Benzene AND Toluene`
    - `NEAR(Benzene Toluene, 10)`
    - `"Exact Phrase"`
    - `Methyl*` (Prefix)
    """)

# --- Main Search Interface ---
query = st.text_input("Enter your search query:", placeholder="e.g. Benzene AND NEAR(Sample Date, 5)", help="FTS5 Syntax supported")

if query:
    df, error = run_query(query, fuzzy=is_fuzzy, limit=limit)
    
    if error:
        st.error(f"**Search Error:** {error}")
        st.info("Tip: If using NEAR, ensure it is uppercase `NEAR(term1 term2, 10)`")
    elif df is not None:
        st.success(f"Found {len(df)} results")
        
        # Format snippet to handle Markdown highlighting (Streamlit dataframe doesn't render MD, so we'll use it for the inspector)
        df['snippet_clean'] = df['snippet'].str.replace('***', '', regex=False)
        
        selection = st.dataframe(
            df[['filename', 'set_name', 'page_number', 'snippet_clean']], 
            column_config={
                "filename": st.column_config.TextColumn("Filename", width="medium"),
                "set_name": st.column_config.TextColumn("Set", width="small"),
                "page_number": st.column_config.NumberColumn("Pg", width="small"),
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
            st.subheader(f"Inspector: {selected_row['filename']} (Page {selected_row['page_number']})")
            
            # PDF Link
            pdf_url = f"https://storage.googleapis.com/fta-form26r-library/full-set/{selected_row['set_name']}/{selected_row['filename'].replace(' ', '%20')}#page={selected_row['page_number']}"
            st.link_button("📂 Open Original PDF in New Tab", pdf_url)
            
            # Fetch full text for the selected page
            conn = get_connection()
            table = "pages_fuzzy" if is_fuzzy else "pages_idx"
            full_text_sql = f"SELECT text FROM {table} WHERE filename = ? AND page_number = ?"
            row = conn.execute(full_text_sql, (selected_row['filename'], int(selected_row['page_number']))).fetchone()
            conn.close()
            
            if row is None:
                st.error(f"Could not retrieve full text for {selected_row['filename']} Page {selected_row['page_number']} from table '{table}'.")
                st.stop()
            full_text = row[0]
            
            with st.expander("📝 Show Full Page Text", expanded=True):
                # Highlight the terms in the full text if possible (simple regex)
                display_text = full_text
                # Try to extract the search terms from the query for highlighting
                search_terms = [t.strip('"') for t in query.split() if len(t) > 2 and t.upper() not in ['AND', 'OR', 'NOT', 'NEAR']]
                for term in search_terms:
                    import re
                    display_text = re.sub(f"({re.escape(term)})", r"**\1**", display_text, flags=re.IGNORECASE)
                
                st.markdown(display_text)
        
        # Download results
        st.divider()
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Results as CSV",
            data=csv,
            file_name=f"search_results_{query.replace(' ', '_')}.csv",
            mime='text/csv',
        )
else:
    st.info("Enter a query to begin searching the 420,000 page corpus.")
