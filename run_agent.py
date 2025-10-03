#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Orchestrates a PO parsing run:
- Downloads all input_urls into /tmp/<RUN-ID>/input
- Detects PDFs by header/bytes (not just file extension)
- Generates debug text dumps for each PDF
- Invokes optional parsers (single_page / multi_page) if present
- Produces output/combined_<RUN-ID>.csv with the expected headers
- Writes structured stdout/stderr logs
Exit codes:
  0 = success (≥1 parsed rows)
  3 = no rows parsed
  1/2 = hard error
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import mimetypes
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Iterable, List, Dict, Optional, Tuple
from urllib.parse import urlparse

# ---- Optional deps (available via requirements.txt) ----
import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from pypdf import PdfReader  # for debug text & basic fallback text extraction
except Exception:  # pragma: no cover
    PdfReader = None


# ---------- CLI & paths ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--input", required=True, help="Input dir")
    p.add_argument("--parsed", required=True, help="Parsed scratch dir")
    p.add_argument("--output", required=True, help="Output dir")
    p.add_argument("--logs", required=True, help="Logs dir")
    p.add_argument("--debug", default=None, help="Debug dir (optional)")
    p.add_argument("--order-number", default=None, help="Optional override for order number")
    return p.parse_args()


def ensure_dir(d: str) -> str:
    os.makedirs(d, exist_ok=True)
    return d


# ---------- Logging to files AND console ----------

def setup_logging(log_dir: str) -> Tuple[logging.Logger, io.StringIO, io.StringIO]:
    ensure_dir(log_dir)
    logger = logging.getLogger("agent")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    # Stream to real stdout/stderr
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("%(message)s"))

    # File handler (same format)
    fh = logging.FileHandler(os.path.join(log_dir, "agent.stdout.txt"), mode="w", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(message)s"))

    # Separate error file
    eh = logging.FileHandler(os.path.join(log_dir, "agent.stderr.txt"), mode="w", encoding="utf-8")
    eh.setLevel(logging.WARNING)
    eh.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(sh)
    logger.addHandler(fh)
    logger.addHandler(eh)
    return logger, stdout_buf, stderr_buf


# ---------- HTTP utilities ----------

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "FR-PO-Agent/1.0"})
    return s


# ---------- PDF detection ----------

def bytes_look_like_pdf(b: bytes) -> bool:
    return b[:5] == b"%PDF-"


def file_looks_like_pdf(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return bytes_look_like_pdf(f.read(5))
    except Exception:
        return False


# ---------- Download & intake ----------

def filename_from_url(url: str, ix: int) -> str:
    try:
        name = os.path.basename(urlparse(url).path)
        name = name or f"file_{ix+1}.pdf"
        # strip query tokens
        name = name.split("?")[0]
    except Exception:
        name = f"file_{ix+1}.pdf"
    return name


def download_all(urls: List[str], input_dir: str, logger: logging.Logger) -> List[str]:
    ensure_dir(input_dir)
    session = make_session()
    saved: List[str] = []

    for i, url in enumerate(urls):
        if not url or not isinstance(url, str):
            logger.warning(f"[download] skipping invalid url at index {i}")
            continue
        name = filename_from_url(url, i)
        dst = os.path.join(input_dir, name)

        try:
            with session.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                # Guess by header first
                ctype = r.headers.get("content-type", "").split(";")[0].strip().lower()
                # Write to disk
                with open(dst, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        if chunk:
                            f.write(chunk)

            # Content sniff
            if not dst.lower().endswith(".pdf"):
                # rename if it actually looks like PDF
                if file_looks_like_pdf(dst) or ctype == "application/pdf":
                    new_dst = dst + ".pdf"
                    os.replace(dst, new_dst)
                    dst = new_dst

            if file_looks_like_pdf(dst):
                logger.info(f"[download] saved PDF: {os.path.basename(dst)}")
                saved.append(dst)
            else:
                logger.warning(f"[download] not a PDF (skipped): {os.path.basename(dst)}")

        except Exception as e:
            logger.warning(f"[download] failed for {url!r}: {e}")

    return saved


def find_pdfs(input_dir: str) -> List[str]:
    out: List[str] = []
    for name in os.listdir(input_dir):
        p = os.path.join(input_dir, name)
        if not os.path.isfile(p):
            continue
        if name.lower().endswith(".pdf") or file_looks_like_pdf(p):
            out.append(p)
    return out


# ---------- Debug text dumps ----------

def extract_text_for_debug(pdf_path: str) -> Tuple[str, str]:
    """
    Returns (raw_text, trimmed_text)
    """
    if PdfReader is None:
        return "", ""

    try:
        reader = PdfReader(pdf_path)
        texts: List[str] = []
        for page in reader.pages:
            try:
                texts.append(page.extract_text() or "")
            except Exception:
                texts.append("")
        raw = "\n".join(texts)

        # 'Trimmed' = collapse multi-space, keep printable, normalize whitespace
        # plus keep only lines that contain alnum or punctuation (avoid empty artifacts)
        lines = []
        for ln in raw.splitlines():
            ln2 = re.sub(r"[ \t]+", " ", ln).strip()
            if ln2:
                lines.append(ln2)
        trimmed = "\n".join(lines)
        return raw, trimmed
    except Exception:
        return "", ""


def write_debug_texts(pdf_path: str, debug_dir: Optional[str], logger: logging.Logger) -> None:
    if not debug_dir:
        return
    ensure_dir(debug_dir)
    base = os.path.basename(pdf_path)
    base_txt = os.path.splitext(base)[0] + ".txt"

    raw, trimmed = extract_text_for_debug(pdf_path)
    if raw:
        with open(os.path.join(debug_dir, f"po_text_{base_txt}"), "w", encoding="utf-8") as f:
            f.write(raw)
    if trimmed:
        with open(os.path.join(debug_dir, f"po_text_trimmed_{base_txt}"), "w", encoding="utf-8") as f:
            f.write(trimmed)
    logger.info(f"[debug] wrote text dumps for {base}")


# ---------- Data model & CSV writer ----------

HEADERS = [
    "Order Number",
    "Part Number",
    "Description",
    "Ordered",
    "Ship-To",
    "Delivery Date",
    "Source File",
    "Page",
]


@dataclass
class Row:
    order_number: str
    part_number: str
    description: str
    ordered: str
    ship_to: str
    delivery_date: str
    source_file: str
    page: str

    def to_list(self) -> List[str]:
        return [
            self.order_number,
            self.part_number,
            self.description,
            self.ordered,
            self.ship_to,
            self.delivery_date,
            self.source_file,
            self.page,
        ]


def write_combined_csv(rows: List[Row], out_dir: str, run_id: str, logger: logging.Logger) -> str:
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"combined_{run_id}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADERS)
        for r in rows:
            w.writerow(r.to_list())
    logger.info(f"[write] combined CSV: {os.path.basename(out_path)} ({len(rows)} rows)")
    return out_path


# ---------- Parser invocation (optional) ----------

def try_import_parsers(logger: logging.Logger):
    sp = None
    mp = None
    try:
        from parsers.single_page import parse_single_page  # type: ignore
        sp = parse_single_page
        logger.info("[parsers] single_page loaded")
    except Exception:
        logger.info("[parsers] single_page not found – skipping")

    try:
        from parsers.multi_page import parse_multi_page  # type: ignore
        mp = parse_multi_page
        logger.info("[parsers] multi_page loaded")
    except Exception:
        logger.info("[parsers] multi_page not found – skipping")

    return sp, mp


# ---------- Main run ----------

def run(run_id: str, input_dir: str, parsed_dir: str, output_dir: str, logs_dir: str,
        debug_dir: Optional[str], order_number_override: Optional[str]) -> int:
    logger, _, _ = setup_logging(logs_dir)
    ensure_dir(input_dir)
    ensure_dir(parsed_dir)
    ensure_dir(output_dir)
    if debug_dir:
        ensure_dir(debug_dir)

    # 1) Pick up list of URLs from control file (created by app.py) or environment
    #    The serving layer posts urls and then shells us with those already placed.
    #    For robustness, also look for urls.json if present.
    urls_manifest = os.path.join(input_dir, "_urls.json")
    urls: List[str] = []
    if os.path.isfile(urls_manifest):
        try:
            with open(urls_manifest, "r", encoding="utf-8") as f:
                urls = json.load(f) or []
        except Exception:
            urls = []

    # If the app passes URLs via env var, accept those too
    if not urls:
        raw = os.environ.get("INPUT_URLS", "")
        if raw:
            try:
                urls = json.loads(raw)
            except Exception:
                urls = [raw]

    # 2) Download all URLs (if any); otherwise assume files already present
    if urls:
        logger.info(f"[intake] downloading {len(urls)} url(s)")
        downloaded = download_all(urls, input_dir, logger)
        if not downloaded:
            logger.warning("[intake] no PDFs downloaded; continuing to scan input dir")

    pdfs = find_pdfs(input_dir)
    logger.info(f"[intake] found {len(pdfs)} PDF(s)")

    if not pdfs:
        logger.error("No PDFs found in input folder")
        return 3

    # 3) Debug text dumps for each
    for p in pdfs:
        write_debug_texts(p, debug_dir, logger)

    # 4) Parse (optional parsers)
    rows: List[Row] = []
    sp, mp = try_import_parsers(logger)

    # We’ll extract order number (fallback to run_id) and pass basics to the parser(s).
    def guess_order_number_from_name(name: str) -> str:
        # e.g., "Purchase-Order-2025-00-34064.pdf"
        m = re.search(r"(\d{4}-\d{2}-\d{5})|\d{2,}-\d{2,}-\d{4,}", name)
        if m:
            return m.group(0)
        # Another style: 2025-00-34064
        m2 = re.search(r"\d{4}-\d{2}-\d{5}", name)
        return m2.group(0) if m2 else run_id

    for pdf_path in pdfs:
        fname = os.path.basename(pdf_path)
        order_no = order_number_override or guess_order_number_from_name(fname)

        # If custom parsers are available, call them; they should return a list of dict rows.
        parsed_any = False
        parsed_dicts: List[Dict[str, str]] = []

        if sp:
            try:
                # expected signature: parse_single_page(pdf_path, order_number, debug_dir) -> list[dict]
                out = sp(pdf_path, order_no, debug_dir)
                if isinstance(out, list):
                    parsed_dicts.extend(out)
                    parsed_any = parsed_any or bool(out)
                    logger.info(f"[single_page] {fname}: {len(out)} rows")
            except Exception as e:
                logger.warning(f"[single_page] {fname}: parser error: {e}")

        if mp:
            try:
                # expected signature: parse_multi_page(pdf_path, order_number, debug_dir) -> list[dict]
                out = mp(pdf_path, order_no, debug_dir)
                if isinstance(out, list):
                    parsed_dicts.extend(out)
                    parsed_any = parsed_any or bool(out)
                    logger.info(f"[multi_page]  {fname}: {len(out)} rows")
            except Exception as e:
                logger.warning(f"[multi_page]  {fname}: parser error: {e}")

        # If neither parser added rows, we still succeed the run but with 0 rows (handled below).
        # Normalize dicts (whatever the parser produced) into Row objects.
        for d in parsed_dicts:
            row = Row(
                order_number=str(d.get("Order Number", order_no) or order_no),
                part_number=str(d.get("Part Number", "") or ""),
                description=str(d.get("Description", "") or ""),
                ordered=str(d.get("Ordered", "") or ""),
                ship_to=str(d.get("Ship-To", "") or ""),
                delivery_date=str(d.get("Delivery Date", "") or ""),
                source_file=str(d.get("Source File", fname) or fname),
                page=str(d.get("Page", "1") or "1"),
            )
            rows.append(row)

        # If your parsers are not yet wired, we at least capture debug text above.

    # 5) Write combined CSV (if any rows)
    if rows:
        write_combined_csv(rows, output_dir, run_id, logger)
        logger.info(f"[done] parsed {len(rows)} row(s) across {len(pdfs)} file(s)")
        return 0

    logger.error("No rows parsed across all files")
    return 3


def main() -> None:
    args = parse_args()

    # Robust directory creation
    input_dir = ensure_dir(args.input)
    parsed_dir = ensure_dir(args.parsed)
    output_dir = ensure_dir(args.output)
    logs_dir = ensure_dir(args.logs)
    debug_dir = ensure_dir(args.debug) if args.debug else None

    try:
        code = run(
            run_id=args.run_id,
            input_dir=input_dir,
            parsed_dir=parsed_dir,
            output_dir=output_dir,
            logs_dir=logs_dir,
            debug_dir=debug_dir,
            order_number_override=args.order_number,
        )
        sys.exit(code)
    except SystemExit as e:
        raise
    except Exception as e:
        # Final safety net – ensure an error is visible in stderr log
        try:
            with open(os.path.join(logs_dir, "agent.stderr.txt"), "a", encoding="utf-8") as f:
                f.write(f"[fatal] {e}\n")
        except Exception:
            pass
        print(f"[fatal] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
