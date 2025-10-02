#!/usr/bin/env python3
"""
run_agent.py
- Reads PDFs
- Parses item rows per page with parsers.single_page
- Enriches each row with: Order Number, Ship-To (City, ST), Delivery Date, Source File, Page
- Writes final CSV with required header order via writers.combined_csv
"""

import argparse, os, re, sys
from typing import List, Dict
from pypdf import PdfReader

from parsers.single_page import parse_single_page
from writers.combined_csv import write_combined_csv

# ---------- ENV knobs ----------
DEBUG_DUMPS     = os.getenv("DEBUG_DUMPS", "1") == "1"
FULL_DEBUG      = os.getenv("FULL_DEBUG", "0") == "1"
DEBUG_REDACT    = os.getenv("DEBUG_REDACT", "1") == "1"
MAX_DEBUG_LINES = int(os.getenv("MAX_DEBUG_LINES", "200"))
# -------------------------------

def _ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)

def read_pages(pdf_path: str) -> List[str]:
    with open(pdf_path, "rb") as f:
        r = PdfReader(f)
        return [(p.extract_text() or "") for p in r.pages]

# --- Debug helpers ---
HEADER_HINTS = re.compile(
    r"(?:^|\b)(part\s*number|item|sku|description|qty|quantity|unit\s*price|extension|amount|ordered)(?:\b|$)",
    re.I,
)
TOTAL_HINTS = re.compile(r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due)\b", re.I)
REDACT_RX   = re.compile(r"(?<!\d)\d{5,}(?!\d)")  # blunt redaction for long numbers

def _trim_table_region(text: str) -> str:
    lines = (text or "").splitlines()
    header_idx = -1
    for i, ln in enumerate(lines):
        if HEADER_HINTS.search(ln or ""):
            header_idx = i
            break
    if header_idx == -1:
        return "\n".join(lines[:MAX_DEBUG_LINES])
    out: List[str] = []
    for ln in lines[header_idx: header_idx + MAX_DEBUG_LINES]:
        out.append(ln)
        if TOTAL_HINTS.search(ln or ""):
            break
    return "\n".join(out)

def _maybe_redact(s: str) -> str:
    return REDACT_RX.sub("[#]", s) if DEBUG_REDACT else s
# ---------------------

# --- Metadata extractors ---
FILENAME_PO_RX = re.compile(r"\b(\d{4}-\d{2}-\d{5})\b")

TEXT_PO_RXES = [
    re.compile(r"(?i)purchase\s*order[:\s-]*\b(\d{4}-\d{2}-\d{5})\b"),
    re.compile(r"(?i)order\s*number[:\s-]*\b(\d{4}-\d{2}-\d{5})\b"),
    re.compile(r"(?i)\bpo\s*#?\s*[:\-]?\s*(\d{4}-\d{2}-\d{5})\b"),
]

CITY_ST_RX = re.compile(r"\b([A-Za-z][A-Za-z .'\-]+),\s*([A-Z]{2})\b")
SHIP_TO_LABEL_RX = re.compile(r"(?i)ship[\s\-]*to\b")
BILL_TO_LABEL_RX = re.compile(r"(?i)bill[\s\-]*to\b")

DATE_LABELED_RXES = [
    re.compile(r"(?i)\b(?:delivery\s*date|deliver\s*by|required\s*date)[:\s]*([0-1]?\d/[0-3]?\d/(?:\d{2}|\d{4}))"),
    re.compile(r"(?i)\b(?:delivery\s*date|deliver\s*by|required\s*date)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})"),
]
DATE_GENERIC_RXES = [
    re.compile(r"\b(0?[1-9]|1[0-2])[\/\-](0?[1-9]|[12][0-9]|3[01])[\/\-](\d{2}|\d{4})\b"),
    re.compile(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b"),
]

def guess_po_number(full_text: str, filename: str) -> str:
    base = os.path.basename(filename)
    m = FILENAME_PO_RX.search(base)
    if m:
        return m.group(1)
    for rx in TEXT_PO_RXES:
        t = rx.search(full_text or "")
        if t:
            return t.group(1)
    return ""

def extract_ship_to_city_state(full_text: str) -> str:
    text = full_text or ""
    m = SHIP_TO_LABEL_RX.search(text)
    if m:
        start = m.end()
        m2 = BILL_TO_LABEL_RX.search(text, pos=start)
        end = m2.start() if m2 else min(len(text), start + 400)
        block = text[start:end]
        city_state = re.findall(r"([A-Za-z][A-Za-z .'\-]+,\s*[A-Z]{2})\b", block)
        if city_state:
            return city_state[-1].strip()
    # fallback: first city/state anywhere
    mcs2 = CITY_ST_RX.search(text)
    return f"{mcs2.group(1).strip()}, {mcs2.group(2)}" if mcs2 else ""

def extract_delivery_date(full_text: str) -> str:
    txt = full_text or ""
    for rx in DATE_LABELED_RXES:
        m = rx.search(txt)
        if m:
            return m.group(1).strip()
    near = re.search(r"(?i)(deliver|ship|required|arrival|eta)[:\s\-]{0,20}([\s\S]{0,120})", txt)
    scope = near.group(2) if near else txt
    for rx in DATE_GENERIC_RXES:
        m = rx.search(scope)
        if m:
            return m.group(0).strip()
    return ""
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="FR PO Agent → Combined CSV")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--input",  required=True)
    ap.add_argument("--parsed", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--logs",   required=True)
    args = ap.parse_args()

    run_id, input_dir, parsed_dir, output_dir, logs_dir = args.run_id, args.input, args.parsed, args.output, args.logs
    debug_dir = os.path.join(os.path.dirname(output_dir), "debug")
    for d in (input_dir, parsed_dir, output_dir, logs_dir, debug_dir):
        _ensure_dir(d)

    pdfs = sorted([f for f in os.listdir(input_dir) if f.lower().endswith(".pdf")])
    if not pdfs:
        print("No PDF files found in input directory", file=sys.stderr, flush=True)
        return 1

    combined_rows: List[Dict[str, str]] = []

    for fname in pdfs:
        fpath = os.path.join(input_dir, fname)
        try:
            pages = read_pages(fpath)
            full_text = "\n".join(pages)

            # Debug dumps
            if DEBUG_DUMPS:
                trimmed = _maybe_redact(_trim_table_region(full_text))
                with open(os.path.join(debug_dir, f"po_text_trimmed_{fname}.txt"), "w", encoding="utf-8", errors="ignore") as dbg:
                    dbg.write(trimmed)
                if FULL_DEBUG:
                    with open(os.path.join(debug_dir, f"po_text_full_{fname}.txt"), "w", encoding="utf-8", errors="ignore") as dbg:
                        dbg.write(_maybe_redact(full_text))

            order_number  = guess_po_number(full_text, fname)
            ship_to_cs    = extract_ship_to_city_state(full_text)  # "City, ST"
            delivery_date = extract_delivery_date(full_text)

            # Parse per-page to attach Page number
            for page_idx, page_text in enumerate(pages, start=1):
                rows = parse_single_page(page_text)
                for r in rows:
                    combined_rows.append({
                        "order_number":  order_number,
                        "part_number":   (r.get("sku") or "").strip(),
                        "description":   (r.get("description") or "").strip(),
                        "ordered":       (r.get("qty") or "").strip(),
                        "ship_to":       ship_to_cs,
                        "delivery_date": delivery_date,
                        "source_file":   fname,
                        "page":          str(page_idx),
                    })

        except Exception as e:
            print(f"{fname}: parse error: {e}", file=sys.stderr, flush=True)

    if combined_rows:
        out_path = os.path.join(output_dir, f"combined_{run_id}.csv")
        write_combined_csv(out_path, combined_rows)
        print(f"Combined CSV written: {out_path} ({len(combined_rows)} rows)", flush=True)
        return 0
    else:
        print("No rows parsed across all files", file=sys.stderr, flush=True)
        return 3

if __name__ == "__main__":
    sys.exit(main())
