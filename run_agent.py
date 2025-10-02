#!/usr/bin/env python3
"""
FR PO Agent → Combined CSV

- Reads PDFs
- Parses line items (parsers.single_page.parse_single_page)
- Enriches with: Order Number, Ship-To (City, ST), Delivery Date, Source File, Page
- Writes final CSV (writers.combined_csv)

ENV:
  DEBUG_DUMPS=1           # write trimmed/full text debug
  FULL_DEBUG=0            # include full text debug
  DEBUG_REDACT=1          # redact long numbers in debug
  MAX_DEBUG_LINES=200
  SHIPTO_DENYLIST="Rochester, NY;Vendor City, ST"   # semicolon-separated
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

# Default denylist excludes vendor HQ (adjust via env)
_deny = os.getenv("SHIPTO_DENYLIST", "Rochester, NY").split(";")
SHIPTO_DENYLIST = {s.strip().upper() for s in _deny if s.strip()}
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

# --- Metadata extractors (more robust) ---
FILENAME_PO_RX = re.compile(r"\b(\d{4}-\d{2}-\d{5})\b")

TEXT_PO_RXES = [
    re.compile(r"(?i)purchase\s*order[:\s-]*\b(\d{4}-\d{2}-\d{5})\b"),
    re.compile(r"(?i)order\s*number[:\s-]*\b(\d{4}-\d{2}-\d{5})\b"),
    re.compile(r"(?i)\bpo\s*#?\s*[:\-]?\s*(\d{4}-\d{2}-\d{5})\b"),
]

CITY_ST_RX = re.compile(r"\b([A-Za-z][A-Za-z .'\-]+),\s*([A-Z]{2})\b")

SHIP_TO_LABEL_RX = re.compile(r"(?i)\b(ship[\s\-]*to|shipping\s*address|deliver\s*to)\b")
# Things that typically end the Ship-To block
SHIP_TO_STOP_RX  = re.compile(
    r"(?i)\b(bill[\s\-]*to|sold[\s\-]*to|remit|vendor|po\s*#|purchase\s*order|terms|phone|fax|email|receiving\s*hours|notes?|comments?|delivery\s*date|required\s*date|ship\s*via)\b"
)

# Delivery date labels we accept
DATE_LABEL_RX = re.compile(
    r"(?i)\b(delivery\s*date|deliver\s*by|required\s*(?:on|by|date)|need\s*by|due\s*date|arrival\s*date|eta)\b"
)

# Date formats
DATE_NUM = r"(?:0?[1-9]|1[0-2])[\/\-\.](?:0?[1-9]|[12][0-9]|3[01])[\/\-](?:\d{4}|\d{2})"
DATE_WORD = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}"
DATE_ANY_RX = re.compile(rf"(?:{DATE_WORD}|{DATE_NUM})")

# Dates we do NOT want (common noise)
NOISY_DATE_LABELS = re.compile(r"(?i)\b(po\s*date|invoice\s*date|order\s*date|print\s*date|run\s*date)\b")

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

def _slice_after_label(text: str, start_rx: re.Pattern, stop_rx: re.Pattern, max_len: int = 1200) -> str:
    """
    Return a slice of text starting at the end of the start label
    and ending at the next stop label (or max_len).
    """
    if not text:
        return ""
    m = start_rx.search(text)
    if not m:
        return ""
    start = m.end()
    sub = text[start : min(len(text), start + max_len)]
    m2 = stop_rx.search(sub)
    return sub[: m2.start()] if m2 else sub

def extract_ship_to_city_state(full_text: str) -> str:
    """
    Prefer City, ST from the explicit Ship-To block.
    Fall back to any City, ST in the doc that is NOT in the denylist.
    """
    txt = full_text or ""

    block = _slice_after_label(txt, SHIP_TO_LABEL_RX, SHIP_TO_STOP_RX, max_len=1600)
    candidates = [f"{m.group(1).strip()}, {m.group(2)}" for m in CITY_ST_RX.finditer(block)]
    # prefer the last in the Ship-To block (often the bottom line)
    for cand in reversed(candidates):
        if cand.upper() not in SHIPTO_DENYLIST:
            return cand

    # Fallback: search entire doc but ignore header/footer noise
    # Heuristic: prioritize City,ST occurrences that appear AFTER a Ship/Deliver hint.
    prefscope = _slice_after_label(txt, re.compile(r"(?i)\b(ship|deliver)\b"), SHIP_TO_STOP_RX, max_len=3000)
    scope_list = [prefscope, txt]
    for scope in scope_list:
        for m in CITY_ST_RX.finditer(scope or ""):
            cand = f"{m.group(1).strip()}, {m.group(2)}"
            if cand.upper() not in SHIPTO_DENYLIST:
                return cand

    return ""  # as a last resort, leave blank rather than returning vendor city

def extract_delivery_date(full_text: str) -> str:
    """
    Look for a labeled delivery/need-by field. Avoid PO/Invoice dates.
    """
    txt = full_text or ""

    # 1) Labeled capture: <label> : <date>
    labeled = re.search(rf"{DATE_LABEL_RX.pattern}\s*[:\-–]?\s*({DATE_ANY_RX.pattern})", txt)
    if labeled:
        # The date is in the last group
        return labeled.group(len(labeled.groups()))

    # 2) Nearby strategy: find a label, then take the first date within ~120 chars after it
    for m in DATE_LABEL_RX.finditer(txt):
        window = txt[m.end(): m.end()+160]
        d = DATE_ANY_RX.search(window)
        if d:
            return d.group(0)

    # 3) Generic: find dates that are NOT preceded by noisy labels
    for d in DATE_ANY_RX.finditer(txt):
        # get a small prefix to check for noisy labels
        prefix_start = max(0, d.start() - 30)
        prefix = txt[prefix_start:d.start()]
        if not NOISY_DATE_LABELS.search(prefix):
            return d.group(0)

    return ""
# ---------------------------------------

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

            # Log chosen meta for troubleshooting
            print(f"[meta] file={fname} order={order_number} ship_to='{ship_to_cs}' delivery='{delivery_date}'", flush=True)

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
