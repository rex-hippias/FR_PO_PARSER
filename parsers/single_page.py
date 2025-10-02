# parsers/single_page.py
from __future__ import annotations
import re
from typing import List, Dict, Tuple, Optional

# --- Heuristics & patterns ---
HEADER_HINTS = re.compile(
    r"(?:^|\b)(part\s*number|item|sku|description|qty|quantity|unit\s*(?:price|cost)|extension|amount|total|ordered)(?:\b|$)",
    re.I,
)
STOP_HINTS = re.compile(
    r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due|receiving\s*hours|alt\s*contact)\b",
    re.I,
)
MONEY_RX = re.compile(r"\$?\(?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2,4})?\)?")
QTY_RX   = re.compile(r"\b\d+(?:\.\d+)?\b")
SKU_STRICT_RX = re.compile(r"\b\d{3,5}-[A-Za-z0-9#]+\b")   # e.g., 0605-####
SKU_LOOSE_RX  = re.compile(r"\b[A-Z0-9][A-Z0-9\-\./]{2,}\b")

def _clean_money(s: str) -> str:
    s = s.strip().replace("$", "").replace(",", "")
    if s.startswith("(") and s.endswith(")"):  # (123.45) -> -123.45
        s = "-" + s[1:-1]
    return s

def _find_header_index(lines: List[str]) -> int:
    for i, ln in enumerate(lines):
        if HEADER_HINTS.search(ln):
            return i
    return 0

def _has_money(s: str) -> bool:
    return bool(re.search(r"\$\s*\d|\d\.\d{2}\)?", s))

def _merge_wrapped_lines(lines: List[str]) -> List[str]:
    """
    Join description lines with the following line when the first line has no $ amount yet.
    This fits the sample where desc is on one line and the numeric tail is on the next.
    """
    merged: List[str] = []
    buf: Optional[str] = None

    for raw in lines:
        ln = raw.strip()
        if not ln:
            continue
        if buf is None:
            if _has_money(ln):
                merged.append(ln)
            else:
                buf = ln
        else:
            # We had a pending desc line; append current line and flush
            ln2 = (buf + " " + ln).strip()
            if not _has_money(ln2):
                # still no money? keep accumulating (rare but safe)
                buf = ln2
            else:
                merged.append(ln2)
                buf = None

    if buf:  # leftover without money; add as-is (will likely be ignored)
        merged.append(buf)
    return merged

def _extract_tail_fields(line: str) -> Tuple[str, str, str]:
    """
    Parse from the tail: last money = unit price; second last = extension;
    qty = last standalone number BEFORE the first money occurrence.
    Returns (qty, ext, unit_price) as strings (cleaned where money).
    """
    # Find $… amounts with positions
    monies = []
    for m in MONEY_RX.finditer(line):
        span = m.span()
        val = _clean_money(m.group(0))
        monies.append((span[0], span[1], val))
    qty = ""
    ext = ""
    unit = ""

    if monies:
        # unit price = last money
        unit = monies[-1][2]
        # ext (if present) = second last money
        if len(monies) >= 2:
            ext = monies[-2][2]

        # qty candidates: numbers whose end is <= start of the FIRST money
        first_money_pos = monies[0][0]
        cand_qty = []
        for q in QTY_RX.finditer(line):
            q_end = q.span()[1]
            if q_end <= first_money_pos:
                cand_qty.append(q.group(0))
        if cand_qty:
            qty = cand_qty[-1]

    return qty, ext, unit

def _find_sku(line: str) -> str:
    m = SKU_STRICT_RX.search(line)
    if m:
        return m.group(0)
    m2 = SKU_LOOSE_RX.search(line)
    return m2.group(0) if m2 else ""

def _strip_tail_tokens(line: str) -> str:
    """
    Remove the trailing tokens we parsed (qty/ext/sku/unit) from the line,
    leaving mostly the description/leading text.
    """
    s = line
    # remove last money (unit)
    s = re.sub(r"(.*)\s+" + MONEY_RX.pattern + r"\s*$", r"\1", s)
    # remove another money (ext)
    s = re.sub(r"(.*)\s+" + MONEY_RX.pattern + r"\s*$", r"\1", s)
    # remove trailing qty
    s = re.sub(r"(.*)\s+" + QTY_RX.pattern + r"\s*$", r"\1", s)
    # remove trailing UOM-like token (e.g., Case500E) if still stuck at the end
    s = re.sub(r"(.*)\s+\b[A-Za-z]{2,}\d{2,}[A-Za-z]*\b\s*$", r"\1", s)
    # remove trailing SKU if present
    s = re.sub(r"(.*)\s+" + SKU_STRICT_RX.pattern + r"\s*$", r"\1", s)
    return s.strip()

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    """
    Returns rows: dicts with keys line_number, sku, qty, price, description.
    (line_number not present in sample → keep empty)
    """
    lines = full_text.splitlines()
    rows: List[Dict[str, str]] = []
    if not lines:
        return rows

    # 1) find header
    hidx = _find_header_index(lines)
    scan = lines[hidx + 1 :]

    # 2) stop at totals/footer
    tail: List[str] = []
    for ln in scan:
        if not ln.strip():
            continue
        if STOP_HINTS.search(ln):
            break
        tail.append(ln)

    # 3) merge wrapped lines (desc then numeric tail)
    merged = _merge_wrapped_lines(tail)

    # 4) parse each merged line by tail-first logic
    for ln in merged:
        if not _has_money(ln):
            continue  # not an item line
        qty, ext, unit = _extract_tail_fields(ln)
        sku = _find_sku(ln)
        desc = _strip_tail_tokens(ln)

        # Minimal signal to accept row: have either unit or ext or qty
        if not (unit or ext or qty):
            continue

        price = unit or ""  # our CSV expects 'price' = unit price
        rows.append({
            "line_number": "",
            "sku": sku,
            "qty": qty,
            "price": price,
            "description": desc,
        })

    return rows
