# parsers/single_page.py
from __future__ import annotations

import re
from typing import List, Dict, Tuple, Optional

# --- Heuristics ---

HEADER_HINTS = re.compile(
    r"(?:^|\b)(line\s*#?|item|sku|description|qty|quantity|unit\s*(?:price|cost)|unit\s*amt|unit\s*amt\.?|price|amount|ext(?:ended)?\s*(?:price|amt|amount)|total)(?:\b|$)",
    re.I,
)
TOTAL_HINTS = re.compile(r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due)\b", re.I)

MONEY_RX = re.compile(r"\(?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?")
QTY_RX   = re.compile(r"\b\d+(?:\.\d+)?\b")
SKU_RX   = re.compile(r"[A-Z0-9][A-Z0-9\-\./]{2,}")

# relaxed line regex variants
LINE_RXES = [
    # line, sku, desc, qty, unit price, (ext optional)
    re.compile(
        r"^\s*(?P<line>\d{1,5})\s+"
        r"(?P<sku>[A-Z0-9][A-Z0-9\-\./]{2,})\s+"
        r"(?P<desc>.+?)\s+"
        r"(?P<qty>\d+(?:\.\d+)?)\s+"
        r"(?P<price>\(?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?)"
        r"(?:\s+(?P<ext>\(?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?))?\s*$",
        re.I,
    ),
    # line, desc, sku, qty, price
    re.compile(
        r"^\s*(?P<line>\d{1,5})\s+"
        r"(?P<desc>.+?)\s+"
        r"(?P<sku>[A-Z0-9][A-Z0-9\-\./]{2,})\s+"
        r"(?P<qty>\d+(?:\.\d+)?)\s+"
        r"(?P<price>\(?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?)\s*$",
        re.I,
    ),
    # no explicit line; sku first
    re.compile(
        r"^\s*(?P<sku>[A-Z0-9][A-Z0-9\-\./]{2,})\s+"
        r"(?P<desc>.+?)\s+"
        r"(?P<qty>\d+(?:\.\d+)?)\s+"
        r"(?P<price>\(?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?)"
        r"(?:\s+(?P<ext>\(?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?))?\s*$",
        re.I,
    ),
]

# column name aliases we’ll search for in the header
COL_ALIASES = {
    "line":       [r"line\s*#?", r"item\s*#?"],
    "sku":        [r"sku", r"item\s*id", r"product\s*id", r"item\s*code", r"upc"],
    "description":[r"descr(?:iption)?", r"item\s*desc", r"description"],
    "qty":        [r"\bqty\b", r"quantity", r"ordered"],
    "price":      [r"unit\s*(?:price|cost|amt|amount)", r"\bprice\b", r"unit\b"],
    "ext":        [r"(?:ext(?:ended)?|line)\s*(?:price|amt|amount|total)", r"amount"],
}

def _clean_money(s: str) -> str:
    s = s.strip().replace("$", "").replace(",", "")
    # handle parentheses for negatives e.g. (12.34)
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    return s

def _find_header_index(lines: List[str]) -> int:
    for i, ln in enumerate(lines):
        if HEADER_HINTS.search(ln):
            return i
    return 0

def _positions_of(patterns: List[str], header: str) -> List[int]:
    pos: List[int] = []
    lower = header.lower()
    for pat in patterns:
        m = re.search(pat, lower, re.I)
        if m:
            pos.append(m.start())
    return pos

def _infer_columns(header: str) -> List[Tuple[str, int]]:
    """
    Return list of (column_key, start_index) sorted by start_index.
    Only columns we actually find in header are returned.
    """
    col_positions: List[Tuple[str, int]] = []
    for key, pats in COL_ALIASES.items():
        hits = _positions_of(pats, header)
        if hits:
            col_positions.append((key, min(hits)))
    # sort by x position
    col_positions.sort(key=lambda t: t[1])
    return col_positions

def _slice_by_cols(line: str, columns: List[Tuple[str, int]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    # build cut ranges from sorted starts
    for idx, (key, start) in enumerate(columns):
        end = columns[idx + 1][1] if idx + 1 < len(columns) else None
        chunk = line[start:end].rstrip("\n")
        out[key] = chunk.strip()
    return out

def _coerce_row(cells: Dict[str, str]) -> Dict[str, str]:
    # normalize output keys
    line_number = cells.get("line", "").strip()
    sku = (cells.get("sku", "") or "").strip()
    desc = (cells.get("description", "") or "").strip()

    # qty preference: explicit qty else last numeric in row
    qty = (cells.get("qty", "") or "").strip()
    if not qty:
        nums = QTY_RX.findall(" ".join(cells.values()))
        qty = nums[-1] if nums else ""

    # price preference: explicit price else last money-looking token
    price = (cells.get("price", "") or "").strip()
    if not price:
        m = None
        for tok in MONEY_RX.findall(" ".join(cells.values())):
            m = tok
        price = m or ""
    price = _clean_money(price) if price else ""

    return {
        "line_number": line_number,
        "sku": sku,
        "qty": qty,
        "price": price,
        "description": desc,
    }

def _parse_line_regex(line: str) -> Optional[Dict[str, str]]:
    for rx in LINE_RXES:
        m = rx.match(line)
        if m:
            gd = m.groupdict()
            return {
                "line_number": (gd.get("line") or "").strip(),
                "sku":        (gd.get("sku") or "").strip(),
                "qty":        (gd.get("qty") or "").strip(),
                "price":      _clean_money((gd.get("price") or "")),
                "description":(gd.get("desc") or "").strip(),
            }
    return None

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    """
    Parse a single-page PO text into line-item rows.
    Returns a list of dicts with keys: line_number, sku, qty, price, description.
    """
    lines = full_text.splitlines()
    rows: List[Dict[str, str]] = []

    if not lines:
        return rows

    header_idx = _find_header_index(lines)
    header = lines[header_idx] if header_idx < len(lines) else ""
    columns = _infer_columns(header)

    # Scan after header
    for ln in lines[header_idx + 1:]:
        if not ln.strip():
            continue
        if TOTAL_HINTS.search(ln):
            break

        # 1) regex attempt
        rec = _parse_line_regex(ln)
        if rec and (rec["sku"] or rec["price"] or rec["qty"]):
            rows.append(rec)
            continue

        # 2) column-slice attempt (only if we found at least 3 columns)
        if len(columns) >= 3:
            cells = _slice_by_cols(ln, columns)
            # discard obvious non-item lines (headers repeated, etc.)
            if any(HEADER_HINTS.search(cells.get(k, "")) for k in cells):
                continue
            rec = _coerce_row(cells)
            # require at least a price or qty or sku to consider it a row
            if rec["sku"] or rec["qty"] or rec["price"]:
                rows.append(rec)

    return rows
