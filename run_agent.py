#!/usr/bin/env python3
# run_agent.py  (Option 1: parsers return rows, writer takes (out_path, rows))

import argparse
import os
import re
import sys
from typing import List, Dict

from pypdf import PdfReader

# your project modules
from parsers.single_page import parse_single_page
from parsers.multi_page import parse_multi_page
from writers.combined_csv import write_combined_csv


# ---------- ENV knobs ----------
DEBUG_DUMPS        = os.getenv("DEBUG_DUMPS", "1") == "1"   # write trimmed debug by default
FULL_DEBUG         = os.getenv("FULL_DEBUG", "0") == "1"    # write full-page dump too
DEBUG_REDACT       = os.getenv("DEBUG_REDACT", "1") == "1"  # redact digit runs in dumps
MAX_DEBUG_LINES    = int(os.getenv("MAX_DEBUG_LINES", "200"))
# -------------------------------

# ---------- logging helpers ----------
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
# --------------------------------------

# ---------- pdf text ----------
def read_pages(pdf_path: str) -> List[str]:
    pages: List[str] = []
    with open(pdf_path, "rb") as f:
        reader = PdfReader(f)
        for p in reader.pages:
            pages.append(p.extract_text() or "")
    return pages
# -----------------------------

# ---------- PO number guess ----------
PO_RXES = [
    re.compile(r"\bPO\s*#\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bPurchase\s*Order\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
    re.compile(r"\bP\.?O\.?\s*[:\-]?\s*([A-Z0-9\-\./]+)", re.I),
]

def guess_po_number(text: str, fallback_filename: str = "") -> str:
    for rx in PO_RXES:
        m = rx.search(text)
        if m:
            return m.group(1).strip()
    return os.path.splitext(fallback_filename)[0]
# -------------------------------------

# ---------- debug trimming / redaction ----------
HEADER_HINTS = re.compile(
    r"(?:^|\b)(line\s*#?|item|sku|description|qty|quantity|unit\s*(?:price|cost)|ext(?:ended)?\s*price)(?:\b|$)",
    re.I,
)
TOTAL_HINTS = re.compile(r"\b(subtotal|total|tax|freight|grand\s*total)\b", re.I)
REDACT_RX   = re.compile(r"(?<!\d)\d{5,}(?!\d)")  # blunt mask for long digit runs

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
# -----------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="FR PO Agent: parse PDFs → combined CSV")
    ap.add_argument("--run-id", required=True, help="Run identifier")
    ap.add_argument("--input",  required=True, help="Input dir (with PDFs)")
    ap.add_argument("--parsed", required=True, help="Parsed dir (not used in Option 1, kept for structure)")
    ap.add_argument("--output", required=True, help="Output dir (combined CSV will be written here)")
    ap.add_argument("--logs",   required=True, help="Logs dir")
    args = ap.parse_args()

    run_id    = args.run_id
    input_dir = args.input
    parsed_dir= args.parsed  # kept for compatibility (not used by this option)
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

            # --- DEBUG DUMPS (trimmed always; full optional) ---
            if DEBUG_DUMPS:
                trimmed = _maybe_redact(_trim_table_region(full_text))
                dbg_trim_path = os.path.join(debug_dir, f"po_text_trimmed_{fname}.txt")
                with open(dbg_trim_path, "w", encoding="utf-8", errors="ignore") as dbg:
                    dbg.write(trimmed)

                if FULL_DEBUG:
                    dbg_full_path = os.path.join(debug_dir, f"po_text_full_{fname}.txt")
                    with open(dbg_full_path, "w", encoding="utf-8", errors="ignore") as dbg:
                        dbg.write(_maybe_redact(full_text))
            # ---------------------------------------------------

            po_number = guess_po_number(full_text, fname)

            # Choose strategy based on page count
            if len(pages) <= 1:
                rows = parse_single_page(full_text)       # expects str → List[dict]
            else:
                rows = parse_multi_page(pages)            # expects List[str] → List[dict]

            if not rows:
                log_err(f"{fname}: no line items matched (see po_text_trimmed_{fname}.txt)", logs_dir)
            else:
                log_out(f"{fname}: parsed {len(rows)} rows (PO={po_number or 'unknown'})", logs_dir)

            # Normalize + accumulate (CSV column names for writer)
            for r in rows:
                combined_rows.append({
                    "po_number": po_number,
                    "file_name": fname,
                    "line_number": str(r.get("line_number", "")).strip(),
                    "sku":        str(r.get("sku", "")).strip(),
                    "qty":        str(r.get("qty", "")).strip(),
                    "price":      str(r.get("price", "")).strip(),
                })

        except Exception as e:
            # ensure there is at least an empty trimmed file for this PDF
            try:
                dbg_trim_path = os.path.join(debug_dir, f"po_text_trimmed_{fname}.txt")
                if not os.path.exists(dbg_trim_path):
                    with open(dbg_trim_path, "w", encoding="utf-8") as dbg:
                        dbg.write("")
            except Exception:
                pass
            log_err(f"{fname}: parse error: {e}", logs_dir)

    # Write combined CSV if we have rows
    if combined_rows:
        out_path = os.path.join(output_dir, f"combined_{run_id}.csv")
        try:
            write_combined_csv(out_path, combined_rows)  # writer signature: (out_path, rows)
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
