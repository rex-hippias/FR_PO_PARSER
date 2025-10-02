# parsers/single_page.py
from __future__ import annotations

import re
from typing import List, Dict

# Heuristics for finding the table header and the end of the item table
HEADER_HINTS = re.compile(
    r"(?:^|\b)(line\s*#?|item|sku|description|qty|quantity|unit\s*(?:price|cost)|ext(?:ended)?\s*price)(?:\b|$)",
    re.I,
)
TOTAL_HINTS = re.compile(r"\b(subtotal|total|tax|freight|grand\s*total)\b", re.I)

# Money/qty/sku helpers
MONEY_RX = re.compile(r"-?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?")
QTY_RX   = re.compile(r"\b\d+(?:\.\d+)?\b")
SKU_RX   = re.compile(r"[A-Z0-9][A-Z0-9\-\./]{2,}")

# Primary line regex (common layout): line  sku  description  qty  unit-price  ext-price
LINE_RXES = [
    re.compile(
        r"^\s*(?P<line>\d{1,4})\s+"
        r"(?P<sku>[A-Z0-9][A-Z0-9\-\./]{2,})\s+"
        r"(?P<desc>.+?)\s+"
        r"(?P<qty>\d+(?:\.\d+)?)\s+"
        r"(?P<price>-?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?)"
        r"(?:\s+(?P<ext>-?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?))?\s*$",
        re.I,
    ),
    # Variant: line [desc chunks ...] sku qty price (some vendors swap order)
    re.compile(
        r"^\s*(?P<line>\d{1,4})\s+"
        r"(?P<desc>.+?)\s+"
        r"(?P<sku>[A-Z0-9][A-Z0-9\-\./]{2,})\s+"
        r"(?P<qty>\d+(?:\.\d+)?)\s+"
        r"(?P<price>-?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$",
        re.I,
    ),
]

def _is_header(line: str) -> bool:
    return bool(HEADER_HINTS.search(line))

def _is_totals(line: str) -> bool:
    return bool(TOTAL_HINTS.search(line))

def _clean_money(s: str) -> str:
    s = s.replace("$", "").replace(",", "").strip()
    return s

def _split_columns(line: str) -> List[str]:
    # Split on 2+ spaces to approximate columns
    parts = re.split(r"\s{2,}", line.strip())
    return [p for p in parts if p]

def _parse_line(line: str) -> Dict[str, str] | None:
    # 1) Try regexes
    for rx in LINE_RXES:
        m = rx.match(line)
        if m:
            gd = m.groupdict()
            line_number = gd.get("line", "") or ""
            sku = gd.get("sku", "") or ""
            qty = gd.get("qty", "") or ""
            price = _clean_money(gd.get("price", "") or "")
            desc = (gd.get("desc", "") or "").strip()
            if desc and len(desc) <= 2 and sku and not SKU_RX.fullmatch(desc):
                # extremely short desc likely not real; ignore
                desc = ""
            return {
                "line_number": line_number.strip(),
                "sku": sku.strip(),
                "qty": qty.strip(),
                "price": price,
                "description": desc,
            }

    # 2) Fallback: split columns
    cols = _split_columns(line)
    if len(cols) >= 4:
        # Heuristic: first col numeric → line; last money → price; somewhere numeric → qty; one token looking like SKU
        cand_line = cols[0] if cols and cols[0].strip().isdigit() else ""
        cand_price = ""
        # look from the end for money
        for tok in reversed(cols):
            if MONEY_RX.search(tok):
                cand_price = _clean_money(MONEY_RX.search(tok).group(0))
                break
        # find qty (a number) preferring tokens before price
        cand_qty = ""
        for tok in cols:
            if QTY_RX.fullmatch(tok.strip()):
                cand_qty = tok.strip()
        # identify sku among tokens
        cand_sku = ""
        for tok in cols:
            t = tok.strip()
            if SKU_RX.fullmatch(t):
                cand_sku = t
                break
        # build a description from middle tokens not used by line/qty/price
        used = {cand_line, cand_sku, cand_qty}
        middle = [t for t in cols[1:-1] if t not in used]
        desc = " ".join(middle).strip()
        if cand_price or cand_qty or cand_sku:
            return {
                "line_number": cand_line,
                "sku": cand_sku,
                "qty": cand_qty,
                "price": cand_price,
                "description": desc,
            }

    return None

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    """
    Parse a single-page PO text into line-item rows.
    Returns a list of dicts with keys: line_number, sku, qty, price, description.
    """
    lines = full_text.splitlines()
    rows: List[Dict[str, str]] = []

    # Find header row
    start = 0
    for i, ln in enumerate(lines):
        if _is_header(ln):
            start = i
            break

    # Scan forward until totals or end
    for ln in lines[start+1:]:
        if not ln.strip():
            continue
        if _is_totals(ln):
            break
        rec = _parse_line(ln)
        if rec:
            rows.append(rec)

    return rows
