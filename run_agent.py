#!/usr/bin/env python3
import argparse, os, re, sys
from typing import List, Dict
from pypdf import PdfReader

from parsers.single_page import parse_single_page
from writers.combined_csv import write_combined_csv

DEBUG_DUMPS     = os.getenv("DEBUG_DUMPS", "1") == "1"
FULL_DEBUG      = os.getenv("FULL_DEBUG", "0") == "1"
DEBUG_REDACT    = os.getenv("DEBUG_REDACT", "1") == "1"
MAX_DEBUG_LINES = int(os.getenv("MAX_DEBUG_LINES", "200"))

def _ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)

def read_pages(pdf_path: str) -> List[str]:
    with open(pdf_path, "rb") as f:
        r = PdfReader(f)
        return [(p.extract_text() or "") for p in r.pages]

# --- Debug helpers (unchanged-ish) ---
HEADER_HINTS = re.compile(
    r"(?:^|\b)(part\s*number|item|sku|description|qty|quantity|unit\s*price|extension|amount|ordered)(?:\b|$)",
    re.I,
)
TOTAL_HINTS = re.compile(r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due)\b", re.I)
REDACT_RX   = re.compile(r"(?<!\d)\d{5,}(?!\d)")

def _trim_table_region(text: str) -> str:
    lines = text.splitlines()
    header_idx = -1
    for i, ln in enumerate(lines):
        if HEADER_HINTS.search(ln):
            header_idx = i
            break
    if header_idx == -1:
        return "\n".join(lines[:MAX_DEBUG_LINES])
    out: List[str] = []
    for ln in lines[header_idx: header_idx + MAX_DEBUG_LINES]:
        out.append(ln)
        if TOTAL_HINTS.search(ln):
            break
    return "\n".join(out)

def _maybe_redact(s: str) -> str:
    return REDACT_RX.sub("[#]", s) if DEBUG_REDACT else s
# ------------------------------------

# --- Metadata extractors (stronger) ---
PO_RXES = [
    re.compile(r"\bPO\s*#?\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bPurchase\s*Order\s*#?\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bOrder\s*Number\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bP\.?O\.?\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
]

CITY_ST_RX = re.compile(r"\b([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Z]{2})\b")
SHIP_TO_LABEL_RX = re.compile(r"ship[\s\-]*to\b", re.I)
BILL_TO_LABEL_RX = re.compile(r"bill[\s\-]*to\b", re.I)

DATE_LABELED_RXES = [
    re.compile(r"\b(?:Delivery\s*Date|Deliver\s*By|Required\s*Date)[:\s]*([0-1]?\d/[0-3]?\d/(?:\d{2}|\d{4}))", re.I),
    re.compile(r"\b(?:Delivery\s*Date|Deliver\s*By|Required\s*Date)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", re.I),
]
DATE_GENERIC_RXES = [
    re.compile(r"\b(0?[1-9]|1[0-2])[\/\-](0?[1-9]|[12][0-9]|3[01])[\/\-](\d{2}|\d{4})\b"),
    re.compile(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b"),
]

def guess_po_number(full_text: str, fallback_filename: str) -> str:
    for rx in PO_RXES:
        m = rx.search(full_text)
        if m:
            return m.group(1).strip()
    # fallback: derive from filename like "Purchase-Order-2025-00-34064.pdf"
    base = os.path.splitext(fallback_filename)[0]
    tail = base.split("Purchase-Order-")[-1] if "Purchase-Order-" in base else base
    return tail

def extract_ship_to_city_state(full_text: str) -> str:
    text = full_text
    m = SHIP_TO_LABEL_RX.search(text)
    if m:
        start = m.end()
        # end at Bill To, or after ~400 chars, whichever first
        m2 = BILL_TO_LABEL_RX.search(text, pos=start)
        end = m2.start() if m2 else min(len(text), start + 400)
        block = text[start:end]
        mcs = CITY_ST_RX.search(block)
        if mcs:
            return f"{mcs.group(1).strip()}, {mcs.group(2)}"
    # fallback: any city/state in doc
    mcs2 = CITY_ST_RX.search(text)
    return f"{mcs2.group(1).strip()}, {mcs2.group(2)}" if mcs2 else ""

def extract_delivery_date(full_text: str) -> str:
    for rx in DATE_LABELED_RXES:
        m = rx.search(full_text)
        if m:
            return m.group(1).strip()
    # fallback: first generic-looking date near "Deliver" or "Ship"
    near = re.search(r"(deliver|ship|required|arrival|eta)[:\s\-]{0,20}([\s\S]{0,120})", full_text, re.I)
    scope = near.group(2) if near else full_text
    for rx in DATE_GENERIC_RXES:
        m = rx.search(scope)
        if m:
            return m.group(0).strip()
    return ""

# ---------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="FR PO Agent: parse PDFs → combined CSV with required headers")
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
        print("No PDF files found in input directory", file=sys.stderr)
        return 1

    combined_rows: List[Dict[str, str]] = []

    for fname in pdfs:
        fpath = os.path.join(input_dir, fname)
        try:
            pages = read_pages(fpath)
            full_text = "\n".join(pages)

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

            print(f"{fname}: parsed {len([r for r in combined_rows if r.get('source_file')==fname])} rows", flush=True)

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
