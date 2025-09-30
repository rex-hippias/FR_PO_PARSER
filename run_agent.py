# run_agent.py
import os
import re
import sys
import argparse
from typing import List, Dict

from pypdf import PdfReader  # pip install pypdf
from parsers.single_page import parse_single_page
from parsers.multi_page import parse_multi_page
from writers.combined_csv import write_combined


PO_PATTERNS = [
    re.compile(r"\bPO(?:\s*#|[:\s])\s*([A-Z0-9\-]+)", re.IGNORECASE),
    re.compile(r"\bPurchase\s*Order(?:\s*#|[:\s])\s*([A-Z0-9\-]+)", re.IGNORECASE),
]

def read_pages(path: str) -> List[str]:
    """Extract text for each page of a PDF (empty string if a page has no text)."""
    reader = PdfReader(path)
    pages: List[str] = []
    for p in reader.pages:
        try:
            pages.append(p.extract_text() or "")
        except Exception:
            # Some pages (forms) may raise; keep pipeline moving
            pages.append("")
    return pages

def guess_po_number(full_text: str, filename: str) -> str:
    for pat in PO_PATTERNS:
        m = pat.search(full_text)
        if m:
            return m.group(1).strip()
    # fallback: derive from filename
    base = os.path.splitext(os.path.basename(filename))[0]
    m = re.search(r"(?:PO|Purchase-Order)[-_ ]?([A-Za-z0-9\-]+)", base, re.IGNORECASE)
    return (m.group(1) if m else base).strip()

def ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--parsed", required=True)  # kept for parity (unused in this baseline)
    ap.add_argument("--output", required=True)
    ap.add_argument("--logs", required=True)
    args = ap.parse_args()

    ensure_dirs(args.output, args.logs)
    debug_dir = os.path.join(os.path.dirname(args.output), "debug")
    ensure_dirs(debug_dir)

    stdout_path = os.path.join(args.logs, "agent.stdout.txt")
    stderr_path = os.path.join(args.logs, "agent.stderr.txt")

    def log_out(msg: str) -> None:
        with open(stdout_path, "a") as f:
            f.write(str(msg).rstrip() + "\n")

    def log_err(msg: str) -> None:
        with open(stderr_path, "a") as f:
            f.write(str(msg).rstrip() + "\n")

    try:
        pdfs = [fn for fn in os.listdir(args.input) if fn.lower().endswith(".pdf")]
    except Exception as e:
        log_err(f"Input directory error: {e}")
        return 2

    if not pdfs:
        log_err("No PDFs found in input/")
        return 2

    combined_rows: List[Dict[str, str]] = []

    for fname in sorted(pdfs):
        fpath = os.path.join(args.input, fname)
        try:
            pages = read_pages(fpath)
            full_text = "\n".join(pages)

            # Write debug dump for inspection/tuning
            dbg_path = os.path.join(debug_dir, f"po_text_{fname}.txt")
            with open(dbg_path, "w") as dbg:
                dbg.write(full_text)

            po_number = guess_po_number(full_text, fname)

            # Choose strategy
            if len(pages) <= 1:
                rows = parse_single_page(full_text)
            else:
                rows = parse_multi_page(pages)

            if not rows:
                log_err(f"{fname}: no line items matched (see {os.path.basename(dbg_path)})")
            else:
                log_out(f"{fname}: parsed {len(rows)} rows (PO={po_number or 'unknown'})")

            # Normalize & enrich
            for r in rows:
                combined_rows.append({
                    "po_number": po_number,
                    "file_name": fname,
                    "line_number": str(r.get("line_number", "")),
                    "sku": str(r.get("sku", "")).strip(),
                    "qty": str(r.get("qty", "")),
                    "price": str(r.get("price", "")),
                })

        except Exception as e:
            log_err(f"{fname}: parse error: {e}")

    out_csv = os.path.join(args.output, f"combined_{args.run_id}.csv")

    try:
        write_combined(combined_rows, out_csv)
    except Exception as e:
        log_err(f"CSV write error: {e}")
        return 2

    if not combined_rows:
        # Header-only CSV was written; signal failure so orchestrator marks job failed
        log_err("No rows parsed across all files")
        return 3

    log_out(f"Wrote {len(combined_rows)} rows → {out_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
