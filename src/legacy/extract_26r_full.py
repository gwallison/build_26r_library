# -*- coding: utf-8 -*-
"""
extract_26r_full.py
-------------------
Extracts Form 26R data from PDFs and generates a consolidated parquet file
and an interactive HTML table.

Outputs:
    data/output/all_harvested_form26r_v2.parquet
    data/output/Form_26R_files_v2.html
"""

import os
import json
import re
import fitz  # PyMuPDF
import pandas as pd
from math import log10, floor

import itables
from itables import init_notebook_mode
init_notebook_mode(all_interactive=True)
from itables import show as iShow
import itables.options as opt
opt.classes="display compact cell-border"
opt.buttons=['pageLength', "copyHtml5", "csvHtml5", ]
opt.maxBytes = 0
opt.allow_html = True
opt.lengthMenu=[2, 5, 10, 50,100]
opt.pageLength=5


def round_sig(x, sig=2, guarantee_str=''):
    try:
        if abs(x) >= 1:
            out = int(round(x, sig - int(floor(log10(abs(x)))) - 1))
            return f"{out:,d}"
        else:
            return round(x, sig - int(floor(log10(abs(x)))) - 1)
    except:
        if guarantee_str:
            return guarantee_str
        return x

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'output')
RESULTS_JSON_DIR = r"D:\PA_Form26r_PDFs\results"  # Intermediate JSON storage
PDF_ROOT = r"D:\PA_Form26r_PDFs\all_pdfs"
DIRSET = ['2010-2018', '2019-2020', '2021-2022',
          '2023_all_months', '2024_all_months',
          'Jan_Jun_2025', 'Jul_Dec_2025']

# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------

def parse_sections_a_and_b(page, current_form):
    """Extracts single-value fields from Section A and B using regex on the native text stream."""
    text_content = page.get_text()

    # --- Header & Section A ---
    date_match = re.search(r'(?:Date Prepared/Revised|Company Name)[\s\S]{1,50}?\b(\d{1,2}/\d{1,2}/\d{2,4})\b', text_content, re.I)
    if date_match:
        current_form["date_prepared"] = date_match.group(1).strip()

    company_match = re.search(r'Company Name[\r\n\s]+([^\r\n]+)', text_content, re.I)
    if company_match:
        val = company_match.group(1).strip()
        if val and not re.match(r'EPA Generator|If a Subsidiary|Company Mailing', val, re.I):
            current_form["company_name"] = val

    loc_match = re.search(r"describe location of waste generation and storage[\.\s]*(.*?)(?=Municipality|County|State|SECTION)", text_content, re.I | re.S)
    if loc_match:
        loc_raw = loc_match.group(1)
        loc_clean = re.sub(r'^(?:\)|Yes|No|X|☑|☐|\s|\.)+', '', loc_raw, flags=re.I).strip()
        loc_clean = re.sub(r'[\r\n]+', ' ', loc_clean).strip()
        if loc_clean:
            current_form["waste_location"] = loc_clean

    # --- Section B: Waste Description ---
    sec_b_match = re.search(r'SECTION B\. WASTE DESCRIPTION[\s\S]*', text_content, re.I)
    if sec_b_match:
        sec_b_block = sec_b_match.group(0)

        waste_match = re.search(r'\b(\d{3})\b\s*\n(.*?)\n\s*([\d\.,]+)\s*\n', sec_b_block, re.S)
        if waste_match:
            current_form["waste_code"] = waste_match.group(1).strip()
            current_form["waste_description"] = waste_match.group(2).strip().replace('\n', ' ')

            raw_amount = waste_match.group(3).strip().replace(',', '')
            try:
                current_form["amount"] = float(raw_amount)
            except ValueError:
                current_form["amount"] = raw_amount

        unit_block_match = re.search(r'(cu yd|gal|ton|lb|Ib).*?(One Time|Continuous)', sec_b_block, re.S | re.I)
        if unit_block_match:
            unit_block = unit_block_match.group(0)
            unit_match = re.search(r'(?:[Xx]I?|☑|☒)\s*(cu yd|gal|ton|lb|Ib)', unit_block, re.I)
            if unit_match:
                current_form["unit"] = unit_match.group(1).lower().replace('ib', 'lb')

    # --- Section 2B: Chemical Characterization ---
    char_match = re.search(r'detailed\s+chemical\s+characterization[\s\S]{1,150}?(?:sampling\s+method|b\.)', text_content, re.I | re.S)
    if char_match:
        char_block = char_match.group(0)

        if re.search(r'(?:[Xx]I?|☑|☒)\s*Yes|Yes\s*(?:[Xx]I?|☑|☒)', char_block, re.I):
            current_form["chemical_characterization_attached"] = True
        elif re.search(r'(?:[Xx]I?|☑|☒)\s*No|No\s*(?:[Xx]I?|☑|☒)', char_block, re.I):
            current_form["chemical_characterization_attached"] = False

    return current_form


def process_pdf_state_machine(filepath):
    """Iterates through PDF pages, identifying forms and extracting appended data."""
    results = {
        "filename": os.path.basename(filepath),
        "needs_ocr": False,
        "error": None,
        "forms": []
    }

    current_form = None

    try:
        doc = fitz.open(filepath)
        for page_num in range(len(doc)):
            page = doc[page_num]
            text_content = page.get_text()

            # 1. OCR Check
            if len(text_content.strip()) < 50 and page_num < 3:
                results["needs_ocr"] = True

            # 2. Form Initialization (Handles multiple forms per file)
            if re.search(r'SECTION\s+A[\.\s]*CLIENT', text_content, re.I):
                if current_form:
                    results["forms"].append(current_form)

                current_form = {
                    "page_number": page_num + 1,
                    "date_prepared": None,
                    "company_name": None,
                    "waste_location": None,
                    "waste_code": None,
                    "waste_description": None,
                    "amount": None,
                    "unit": None,
                    "chemical_characterization_attached": None,
                    "facilities": []
                }
                current_form = parse_sections_a_and_b(page, current_form)

            # 3. Facility Iteration (Appended Page Capture)
            facility_blocks = re.split(r'permit\s+number[s\(\)]*[\s\S]{1,80}?utili[zs]ed\.?', text_content, flags=re.I)

            if current_form and len(facility_blocks) > 1:
                for block in facility_blocks[1:]:
                    block = re.split(r'2\.\s+BENEFICIAL\s+USE', block, flags=re.I)[0]

                    facility = {
                        "permit_numbers": None,
                        "facility_name": None,
                        "address": None,
                        "volume_shipped": None,
                        "volume_unit": None
                    }

                    permit_match = re.search(r'^[\r\n\s]*(.*?)(?=b\.|Facility\s+Name)', block, re.I | re.S)
                    if permit_match:
                        val = re.sub(r'^a\.[\s\r\n]*', '', permit_match.group(1).strip(), flags=re.I).strip()
                        if val: facility['permit_numbers'] = val

                    name_match = re.search(r'Facility\s+Name[\r\n\s]+(.*?)(?=Address\s+Line|C\.|Municipality)', block, re.I | re.S)
                    if name_match:
                        val = name_match.group(1).strip()
                        val = re.sub(r'^b[\.\r\n\s]+', '', val, flags=re.I).strip()
                        if val and not re.match(r'Address\s+Line', val, re.I):
                            facility['facility_name'] = val

                    addr_match = re.search(r'Address\s+Line\s*1[\r\n\s]+(.*?)(?=Municipality|County|C\.)', block, re.I | re.S)
                    if addr_match:
                        raw_addr = addr_match.group(1).strip()
                        clean_addr = re.sub(r'Address\s+Line\s*2|Address\s+City\s+State\s+ZIP', '', raw_addr, flags=re.I)
                        clean_addr = re.sub(r'[\r\n\s]+', ' ', clean_addr).strip()
                        if clean_addr: facility['address'] = clean_addr

                    vol_match = re.search(r'facility\s+in\s+the\s+previous\s+year\.[\r\n\s]*([\d\.,]+)', block, re.I)
                    if vol_match:
                        raw_vol = vol_match.group(1).replace(',', '')
                        try:
                            facility['volume_shipped'] = float(raw_vol)
                        except ValueError:
                            facility['volume_shipped'] = raw_vol

                    unit_match = re.search(r'([X|x|☑]\s*)(cu\s+yd|gal|ton|lb)', block, re.I)
                    if unit_match:
                        facility['volume_unit'] = unit_match.group(2).lower()

                    if facility['facility_name'] or facility['permit_numbers'] or facility['volume_shipped']:
                        current_form["facilities"].append(facility)

        if current_form:
            results["forms"].append(current_form)

    except Exception as e:
        results["error"] = str(e)
    finally:
        if 'doc' in locals():
            doc.close()

    return results


def json_to_flat_parquet(json_filepath, dirname, parquet_filepath, save_parq=True):
    """Transforms hierarchical JSON into a flat DataFrame including page numbers."""
    with open(json_filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    flat_records = []

    for file_record in data:
        forms = file_record.get("forms", [])
        if not forms:
            continue

        filename = file_record.get("filename")
        needs_ocr = file_record.get("needs_ocr")
        error = file_record.get("error")

        for form in forms:
            base_row = {
                "filename": filename,
                "page_number": form.get("page_number"),
                "needs_ocr": needs_ocr,
                "error": error,
                "date_prepared": form.get("date_prepared"),
                "company_name": form.get("company_name"),
                "waste_location": form.get("waste_location"),
                "waste_code": form.get("waste_code"),
                "waste_description": form.get("waste_description"),
                "amount": form.get("amount"),
                "unit": form.get("unit"),
                "chemical_characterization_attached": form.get("chemical_characterization_attached")
            }

            facilities = form.get("facilities", [])

            if not facilities:
                row = base_row.copy()
                row.update({
                    "permit_numbers": None, "facility_name": None, "address": None,
                    "volume_shipped": None, "volume_unit": None
                })
                flat_records.append(row)
            else:
                for facility in facilities:
                    row = base_row.copy()
                    row.update({
                        "permit_numbers": facility.get("permit_numbers"),
                        "facility_name": facility.get("facility_name"),
                        "address": facility.get("address"),
                        "volume_shipped": facility.get("volume_shipped"),
                        "volume_unit": facility.get("volume_unit")
                    })
                    flat_records.append(row)

    df = pd.DataFrame(flat_records)
    df['set_name'] = dirname

    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    if 'volume_shipped' in df.columns:
        df['volume_shipped'] = pd.to_numeric(df['volume_shipped'], errors='coerce')

    if save_parq:
        df.to_parquet(parquet_filepath, index=False)

    return df


def run_test_fixture(input_dir, output_dir, setname):
    if not os.path.exists(input_dir):
        print(f"Error: Target directory does not exist: {input_dir}")
        return
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"form26r_extraction_results__{setname}.json")
    pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    if not pdf_files:
        return

    all_results = []
    print(f"Starting processing of {len(pdf_files)} PDFs in {setname}...")
    for idx, filename in enumerate(pdf_files, 1):
        filepath = os.path.join(input_dir, filename)
        pdf_data = process_pdf_state_machine(filepath)
        all_results.append(pdf_data)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=4)
    print(f"Extraction complete for {setname}.")


def harvest_all_dirs(root_dir, res_dir, dir_set):
    for dset in dir_set:
        curr_dir = os.path.join(root_dir, dset)
        run_test_fixture(curr_dir, res_dir, dset)


def all_json_to_concat(dirset, rootdir, resdir, out_dir=None):
    """Concatenates per-set JSONs into a single parquet.
    resdir: where the per-set JSON files live
    out_dir: where to write the final parquet (defaults to resdir)
    """
    if out_dir is None:
        out_dir = resdir
    alldf = []
    for dset in dirset:
        print(f"Concatenating {dset}...")
        jfn = os.path.join(resdir, f'form26r_extraction_results__{dset}.json')
        if os.path.exists(jfn):
            alldf.append(json_to_flat_parquet(json_filepath=jfn, dirname=dset, parquet_filepath="", save_parq=False))
    if alldf:
        final = pd.concat(alldf)
        os.makedirs(out_dir, exist_ok=True)
        parq_path = os.path.join(out_dir, 'all_harvested_form26r.parquet')
        final.to_parquet(parq_path)
        return parq_path
    return None


# ---------------------------------------------------------------------------
# HTML table generation
# ---------------------------------------------------------------------------

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


def get_link(row):
    rooturl = "https://storage.googleapis.com/fta-form26r-library/full-set/"
    fn = row.filename.replace(' ', '%20')
    pn = f'#page={row.page_number}'
    url = f"""<a href={rooturl}{row.set_name}/{fn}{pn} target="_blank">Open PDF</a>"""
    return url


def make_form26r_html(parquet_path, output_dir):
    """Reads the consolidated parquet and writes an interactive HTML table to output_dir."""
    init_notebook_mode(all_interactive=True, connected=True)

    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file not found: {parquet_path}")
        return

    alldf = pd.read_parquet(parquet_path)
    print(f"Loaded {len(alldf)} rows from {parquet_path}")

    alldf['pdf_link'] = alldf.apply(lambda row: get_link(row), axis=1)
    out = alldf.copy()
    out.volume_shipped = out.volume_shipped.map(lambda x: round_sig(x, 3))

    html = itables.to_html_datatable(
        out[['pdf_link', 'page_number', 'company_name', 'waste_location', 'waste_code',
             'waste_description', 'facility_name', 'address',
             'volume_shipped', 'volume_unit', 'filename', 'set_name', 'date_prepared']].reset_index(drop=True),
        connected=True,
        pageLength=10,
        display_logo_when_loading=False,
        lengthMenu=[2, 5, 10, 50, 100],
        buttons=['pageLength', 'copyHtml5', 'csvHtml5'],
        columnControl=["order", ["orderAsc", "orderDesc", "search"]]
    )

    title = """
    <div class="title-container">
    <h2>All files with Form26R</h2>
    <h3>Each row is a DESTINATION on the form.</h3>
    Therefore there may be more than one row for a given 26R Form.
    </div>
"""

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, 'Form_26R_files_v2.html')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"<html><head>{table_styling}</head><body>")
        f.write(title + html)
        f.write("</body></html>")
    print(f"HTML written to {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Starting full extraction...")
    print(f"Output directory: {OUTPUT_DIR}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Extract PDFs to JSON
    harvest_all_dirs(root_dir=PDF_ROOT, res_dir=RESULTS_JSON_DIR, dir_set=DIRSET)

    # 2. Concatenate JSONs to a single Parquet
    parquet_path = all_json_to_concat(
        dirset=DIRSET,
        rootdir=PDF_ROOT,
        resdir=RESULTS_JSON_DIR,
        out_dir=OUTPUT_DIR
    )

    # 3. Generate HTML table
    if parquet_path:
        make_form26r_html(
            parquet_path=parquet_path,
            output_dir=OUTPUT_DIR
        )
    else:
        print("No data extracted; skipping HTML generation.")
