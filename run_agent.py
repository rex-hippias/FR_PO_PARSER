#!/usr/bin/env python3
# run_agent.py  (Option 1 + metadata enrichment: Order #, Ship-To, Delivery Date, File, Page)

import argparse
import os
import re
import sys
from typing import List, Dict

from pypdf import PdfReader

# Project modules (parsers return rows; writer takes (out_path, rows))
from parsers.single_page import parse_single_page
from writers.combined_csv import write_combined_csv

# ---------- ENV knobs ----------
DEBUG_DUMPS        = os.getenv("DEBUG_DUMPS", "1") == "1"
FULL_DEBUG         = os.getenv("FULL_DEBUG", "0") == "1"
DEBUG_REDACT       = os.getenv("DEBUG_REDACT", "1") == "1"
MAX_DEBUG_LINES    = int(os.getenv("MAX_DEBUG_LINES", "200"))
# -------------------------------

# ---------- helpers ----------
def _ensure_dir(d: str) -> None:
    os.makedirs(d, exist_ok=True)

def _log_path(root: str, name: str) -> str:
    _ensure_dir(root)
    return os.path.join(root, name)

def log_out(msg: str, logs_dir: str):
    print(msg, flush=True)
    with open(_log_path(logs_dir, "agent.stdout.txt"), "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")

def log_err(msg: str, logs_dir: str):
    print(msg, file=sys.stderr, flush=True)
    with open(_log_path(logs_dir, "agent.stderr.txt"), "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")

def read_pages(pdf_path: str) -> List[str]:
    pages: List[str] = []
    with open(pdf_path, "rb") as f:
        reader = PdfReader(f)
        for p in reader.pages:
            pages.append(p.extract_text() or "")
    return pages
# --------------------------------

# ---------- redaction + trim for debug ----------
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
# -------------------------------------------------

# ---------- metadata extractors ----------
PO_RXES = [
    re.compile(r"\bPO\s*#\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bPurchase\s*Order\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bOrder\s*Number\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bP\.?O\.?\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
]

CITY_ST_RX = re.compile(r"\b([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Z]{2})\b")  # City, ST
# Look for a Ship-To block first, then city/state
SHIPTO_BLOCK_RX = re.compile(r"ship[\s\-]*to[:\s]*([\s\S]{0,200})", re.I)

DATE_RXES = [
    re.compile(r"\b(?:Delivery\s*Date|Deliver\s*By|Required\s*Date)[:\s]*([0-1]?\d/[0-3]?\d/[0-9]{2,4})", re.I),
    re.compile(r"\b(?:Delivery\s*Date|Deliver\s*By|Required\s*Date)[:\s]*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", re.I),
]

def guess_po_number(full_text: str, fallback_filename: str) -> str:
    for rx in PO_RXES:
        m = rx.search(full_text)
        if m:
            return m.group(1).strip()
    return os.path.splitext(fallback_filename)[0]

def extract_ship_to_city_state(full_text: str) -> str:
    # Try to focus on the Ship-To block
    m = SHIPTO_BLOCK_RX.search(full_text)
    scope = m.group(1) if m else full_text
    m2 = CITY_ST_RX.search(scope)
    return f"{m2.group(1).strip()}, {m2.group(2)}" if m2 else ""

def extract_delivery_date(full_text: str) -> str:
    for rx in DATE_RXES:
        m = rx.search(full_text)
        if m:
            return m.group(1).strip()
    return ""
# ----------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="FR PO Agent: parse PDFs → combined CSV with metadata")
    ap.add_argument("--run-id", required=True, help="Run identifier")
    ap.add_argument("--input",  required=True, help="Input dir (with PDFs)")
    ap.add_argument("--parsed", required=True, help="Parsed dir (kept for structure)")
    ap.add_argument("--output", required=True, help="Output dir (combined CSV will be written here)")
    ap.add_argument("--logs",   required=True, help="Logs dir")
    args = ap.parse_args()

    run_id    = args.run_id
    input_dir = args.input
    parsed_dir= args.parsed
    output_dir= args.output
    logs_dir  = args.logs
    debug_dir = os.path.join(os.path.dirname(output_dir), "debug")

    for d in (input_dir, parsed_dir, output_dir, logs_dir, debug_dir):
        _ensure_dir(d)

    pdfs = [f for f in os.listdir(input_dir) if f.lower().endswith(".pdf")]
    pdfs.sort()
    if not pdfs:
        log_err("No PDF files found in input directory", logs_dir)
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

            # Document-level metadata (reused for all rows from this file)
            order_number  = guess_po_number(full_text, fname)
            ship_to_cs    = extract_ship_to_city_state(full_text)  # "City, ST"
            delivery_date = extract_delivery_date(full_text)

            # Parse **per-page** so we can record page index
            for page_idx, page_text in enumerate(pages, start=1):
                rows = parse_single_page(page_text)  # returns item dicts
                if not rows:
                    continue

                for r in rows:
                    combined_rows.append({
                        # required by your final CSV spec:
                        "order_number":  order_number,
                        "part_number":   (r.get("sku") or "").strip(),
                        "description":   (r.get("description") or "").strip(),
                        "ordered":       (r.get("qty") or "").strip(),
                        "ship_to":       ship_to_cs,
                        "delivery_date": delivery_date,
                        "source_file":   fname,
                        "page":          str(page_idx),
                    })

                log_out(f"{fname} p.{page_idx}: parsed {len(rows)} rows (PO={order_number or 'unknown'})", logs_dir)

        except Exception as e:
            # Still produce an empty trimmed file if possible
            try:
                dbg_path = os.path.join(debug_dir, f"po_text_trimmed_{fname}.txt")
                if not os.path.exists(dbg_path):
                    with open(dbg_path, "w", encoding="utf-8") as dbg:
                        dbg.write("")
            except Exception:
                pass
            log_err(f"{fname}: parse error: {e}", logs_dir)

    # Write combined CSV
    if combined_rows:
        out_path = os.path.join(output_dir, f"combined_{run_id}.csv")
        try:
            write_combined_csv(out_path, combined_rows)  # writer maps to desired header order
            log_out(f"Combined CSV written: {out_path} ({len(combined_rows)} rows)", logs_dir)
            return 0
        except Exception as e:
            log_err(f"Failed to write combined CSV: {e}", logs_dir)
            return 1
    else:
        log_err("No rows parsed across all files", logs_dir)
        return 3


if __name__ == "__main__":
    sys.exit(main())
