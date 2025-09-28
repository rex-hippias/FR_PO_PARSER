# run_agent.py
import os, re, csv, sys, argparse
from typing import List, Dict
from pypdf import PdfReader  # pip install pypdf

LINE_PATTERNS = [
    # Common PO row: line  SKU/Item     QTY   PRICE (very generic; adjust to your format)
    re.compile(r"^\s*(\d{1,4})\s+([A-Z0-9\-_/]+)\s+(\d{1,6})\s+(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$"),
    # Variant: line then description then qty price
    re.compile(r"^\s*(\d{1,4})\s+([^\s].*?)\s+(\d{1,6})\s+(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$"),
]

PO_PATTERNS = [
    re.compile(r"\bPO(?:\s*#|[:\s])\s*([A-Z0-9\-]+)", re.IGNORECASE),
    re.compile(r"\bPurchase\s*Order(?:\s*#|[:\s])\s*([A-Z0-9\-]+)", re.IGNORECASE),
]

def read_pdf_text(path: str) -> str:
    try:
        reader = PdfReader(path)
        parts = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        return "\n".join(parts)
    except Exception as e:
        raise RuntimeError(f"Failed to read {os.path.basename(path)}: {e}")

def guess_po_number(text: str) -> str:
    for pat in PO_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(1).strip()
    return ""  # unknown

def parse_lines(text: str) -> List[Dict[str, str]]:
    rows = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        for pat in LINE_PATTERNS:
            m = pat.match(line)
            if m:
                rows.append({
                    "line_number": m.group(1),
                    "sku": m.group(2),
                    "qty": m.group(3),
                    "price": m.group(4),
                })
                break
    return rows

def write_csv(rows: List[Dict[str, str]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fieldnames = ["po_number", "file_name", "line_number", "sku", "qty", "price"]
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--parsed", required=True)  # for parity (unused here)
    ap.add_argument("--output", required=True)
    ap.add_argument("--logs", required=True)
    args = ap.parse_args()

    os.makedirs(args.output, exist_ok=True)
    os.makedirs(args.logs, exist_ok=True)
    stdout_path = os.path.join(args.logs, "agent.stdout.txt")
    stderr_path = os.path.join(args.logs, "agent.stderr.txt")

    def log_out(msg: str):
        with open(stdout_path, "a") as f: f.write(msg.rstrip() + "\n")
    def log_err(msg: str):
        with open(stderr_path, "a") as f: f.write(msg.rstrip() + "\n")

    combined_rows: List[Dict[str, str]] = []
    pdfs = [p for p in os.listdir(args.input) if p.lower().endswith(".pdf")]
    if not pdfs:
        log_err("No PDFs found in input/")
        return 2

    for fname in pdfs:
        fpath = os.path.join(args.input, fname)
        try:
            text = read_pdf_text(fpath)
            po = guess_po_number(text) or ""
            rows = parse_lines(text)
            if not rows:
                log_err(f"{fname}: no line items matched baseline patterns")
            for r in rows:
                r_full = {
                    "po_number": po,
                    "file_name": fname,
                    "line_number": r.get("line_number",""),
                    "sku": r.get("sku",""),
                    "qty": r.get("qty",""),
                    "price": r.get("price",""),
                }
                combined_rows.append(r_full)
            log_out(f"{fname}: parsed {len(rows)} rows (PO={po or 'unknown'})")
        except Exception as e:
            log_err(f"{fname}: parse error: {e}")

    out_csv = os.path.join(args.output, f"combined_{args.run_id}.csv")
    if not combined_rows:
        log_err("No rows parsed across all files")
        # still write an empty CSV with header so downstream can see a file
        write_csv([], out_csv)
        return 3  # non-zero → API marks as failed

    write_csv(combined_rows, out_csv)
    log_out(f"Wrote {len(combined_rows)} rows to {out_csv}")
    return 0

if __name__ == "__main__":
    code = main()
    sys.exit(code)
