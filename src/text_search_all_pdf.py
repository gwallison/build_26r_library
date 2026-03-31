import os
import re
import sys

import fitz  # PyMuPDF
import pandas as pd

import itables
from itables import init_notebook_mode
import itables.options as opt

init_notebook_mode(all_interactive=True)
opt.classes = "display compact cell-border"
opt.buttons = ['pageLength', 'copyHtml5', 'csvHtml5']
opt.maxBytes = 0
opt.allow_html = True
opt.lengthMenu = [2, 5, 10, 50, 100]
opt.pageLength = 10

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
DIRSET = ['2010-2018', '2019-2020', '2021-2022',
          '2023_all_months', '2024_all_months',
          'Jan_Jun_2025', 'Jul_Dec_2025']
ROOTURL = "https://storage.googleapis.com/fta-form26r-library/full-set/"
TMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmp')
CORPUS_PATH = os.path.join(TMP_DIR, 'pdf_corpus.parquet')

table_styling = """<style>
  body {
    font-family: "Noto Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell,
                 "Fira Sans", "Droid Sans", "Helvetica Neue", sans-serif;
    font-size: 16px;
    color: rgb(9, 66, 100);
  }
  table, th, td {
    border-color: rgb(214, 239, 238);
  }
  th {
    font-weight: 400;
  }
</style>"""


# ---------------------------------------------------------------------------
# Corpus-based search (fast path)
# ---------------------------------------------------------------------------

def search_corpus(corpus_path: str, search_text: str) -> list:
    """Search the pre-built page corpus. Returns same match-record format as search_all_pdfs."""
    df = pd.read_parquet(corpus_path)
    mask = df['text'].str.contains(search_text, case=False, na=False, regex=False)
    hits = df[mask]

    matches = []
    for (set_name, filename), group in hits.groupby(['set_name', 'filename'], sort=False):
        pages = sorted(group['page_number'].tolist())
        matches.append({
            "filename": filename,
            "set_name": set_name,
            "first_page": pages[0],
            "pages_found": pages,
            "match_count": len(pages),
        })
    return matches


# ---------------------------------------------------------------------------
# Direct PDF search (slow fallback)
# ---------------------------------------------------------------------------

def search_pdf(pdf_path: str, search_text: str) -> list:
    """Return sorted list of 1-based page numbers where search_text appears (case-insensitive)."""
    needle = search_text.lower()
    try:
        doc = fitz.open(pdf_path)
        matched_pages = []
        for page_index in range(len(doc)):
            page = doc[page_index]
            if needle in page.get_text().lower():
                matched_pages.append(page_index + 1)  # 1-based
        doc.close()
        return matched_pages
    except Exception as e:
        print(f"  WARNING: could not read {pdf_path}: {e}")
        return []


def search_all_pdfs(root: str, dirset: list, search_text: str) -> list:
    """Walk all PDFs in root/set_name directories and return match records."""
    matches = []
    for set_name in dirset:
        dir_path = os.path.join(root, set_name)
        if not os.path.isdir(dir_path):
            print(f"  Skipping missing directory: {dir_path}")
            continue
        pdf_files = sorted(
            f for f in os.listdir(dir_path) if f.lower().endswith('.pdf')
        )
        for fname in pdf_files:
            pdf_path = os.path.join(dir_path, fname)
            print(f"  Searching: {set_name}/{fname}")
            pages = search_pdf(pdf_path, search_text)
            if pages:
                matches.append({
                    "filename": fname,
                    "set_name": set_name,
                    "first_page": pages[0],
                    "pages_found": pages,
                    "match_count": len(pages),
                })
    return matches


# ---------------------------------------------------------------------------
# HTML output
# ---------------------------------------------------------------------------

def _get_link(row) -> str:
    fn = str(row.filename).replace(" ", "%20")
    url = f"{ROOTURL}{row.set_name}/{fn}#page={row.first_page}"
    return f'<a href={url} target="_blank">{row.filename}</a>'


def build_html(matches: list, search_text: str, output_dir: str) -> str:
    """Build an itables HTML file from match records; return the output path."""
    init_notebook_mode(all_interactive=True, connected=True)

    df = pd.DataFrame(matches)
    df['pdf_link'] = df.apply(_get_link, axis=1)
    df['pages_found_str'] = df['pages_found'].apply(
        lambda pages: ", ".join(str(p) for p in pages)
    )

    html = itables.to_html_datatable(
        df[['pdf_link', 'set_name', 'pages_found_str', 'match_count']].reset_index(drop=True),
        connected=True,
        pageLength=10,
        display_logo_when_loading=False,
        lengthMenu=[2, 5, 10, 50, 100],
        buttons=['pageLength', 'copyHtml5', 'csvHtml5'],
        columnControl=["order", ["orderAsc", "orderDesc", "search"]]
    )

    title = f"""
    <div class="title-container">
    <h2>Text search results: &ldquo;{search_text}&rdquo;</h2>
    <h3>{len(matches)} file(s) matched &mdash; link opens PDF at first matching page.</h3>
    </div>
"""

    safe_name = re.sub(r'[^\w]', '_', search_text)
    out_path = os.path.join(output_dir, f'text_search_{safe_name}.html')
    os.makedirs(output_dir, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"<html><head>{table_styling}</head><body>")
        f.write(title + html)
        f.write("</body></html>")
    return out_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    search_text = sys.argv[1] if len(sys.argv) > 1 else "geochemical testing"

    print(f'Searching for: "{search_text}"')

    if os.path.exists(CORPUS_PATH):
        print(f"Using corpus: {CORPUS_PATH}")
        matches = search_corpus(CORPUS_PATH, search_text)
    else:
        print(f"No corpus found at {CORPUS_PATH}.")
        print("Tip: run  python build_pdf_corpus.py  once to enable fast searches.")
        print(f"Falling back to direct PDF search of: {ROOT}")
        print()
        matches = search_all_pdfs(ROOT, DIRSET, search_text)

    print(f"\n{len(matches)} file(s) matched.")
    if matches:
        out = build_html(matches, search_text, TMP_DIR)
        print(f"Results written to: {out}")
