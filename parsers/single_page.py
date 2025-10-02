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

# Money, qty, tokens with positions
MONEY_RX = re.compile(r"\$?\(?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2,4})?\)?")
# strictly "standalone" numeric (won't grab the 500 from "Case500E")
NUM_TOKEN = re.compile(r"(?<![A-Za-z0-9])\d+(?:\.\d+)?(?![A-Za-z0-9])")

# Strict sample style: 0605-#### ; allow broader code fallback
SKU_STRICT_RX = re.compile(r"\b\d{3,5}-[A-Za-z0-9#]+\b")   # e.g., 0605-2507104
SKU_LOOSE_RX  = re.compile(r"\b[A-Z0-9][A-Z0-9\-\./]{2,}\b")

# UOM-like “word+digits(+word)”, e.g., Case500E
UOM_RX = re.compile(r"\b[A-Za-z]{2,}\d{2,}[A-Za-z]*\b")

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
    Fits the sample where desc is on one line and the numeric tail is on the next.
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
            ln2 = (buf + " " + ln).strip()
            if not _has_money(ln2):
                buf = ln2
            else:
                merged.append(ln2)
                buf = None

    if buf:
        merged.append(buf)
    return merged

def _extract_positions(line: str):
    monies = [(m.start(), m.end(), _clean_money(m.group(0))) for m in MONEY_RX.finditer(line)]
    skus   = [(m.start(), m.end(), m.group(0)) for m in SKU_STRICT_RX.finditer(line)]
    if not skus:  # fallback
        skus = [(m.start(), m.end(), m.group(0)) for m in SKU_LOOSE_RX.finditer(line)]
    uoms   = [(m.start(), m.end(), m.group(0)) for m in UOM_RX.finditer(line)]
    nums   = [(m.start(), m.end(), m.group(0)) for m in NUM_TOKEN.finditer(line)]
    return monies, skus, uoms, nums

def _pick_tail_fields(line: str) -> Tuple[str, str, str, int, int, int, int]:
    """
    Decide qty, extension, unit, and return their *start* positions for trimming.
    Returns: (qty, ext, unit, pos_qty, pos_ext, pos_unit, pos_first_money)
    """
    monies, skus, uoms, nums = _extract_positions(line)
    qty = ext = unit = ""
    pos_qty = pos_ext = pos_unit = 10**9
    pos_first_money = 10**9

    if monies:
        pos_unit = monies[-1][0]                 # last money → unit
        unit     = monies[-1][2]
        if len(monies) >= 2:
            pos_ext = monies[-2][0]              # second last money → extension
            ext     = monies[-2][2]
        pos_first_money = monies[0][0]

        # qty = last standalone number whose END is before the FIRST money
        cand = [n for (s,e,v) in nums if e <= pos_first_money]
        if cand:
            s,e,v = cand[-1]
            qty   = v
            pos_qty = s

    return qty, ext, unit, pos_qty, pos_ext, pos_unit, pos_first_money

def _find_sku_pos(line: str) -> Tuple[str, int]:
    for rx in (SKU_STRICT_RX, SKU_LOOSE_RX):
        m = rx.search(line)
        if m:
            return m.group(0), m.start()
    return "", 10**9

def _find_last_uom_before(line: str, cutoff: int) -> Tuple[str, int]:
    last = ("", 10**9)
    for m in UOM_RX.finditer(line):
        if m.end() <= cutoff:
            last = (m.group(0), m.start())
    return last

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    """
    Returns rows: dicts with keys:
      sku, description, qty, price (unit), ext (extension), uom
    """
    lines = full_text.splitlines()
    rows: List[Dict[str, str]] = []
    if not lines:
        return rows

    # 1) find header
    hidx = _find_header_index(lines)
    scan = lines[hidx + 1 :]

    # 2) stop at totals/footer signals
    useful: List[str] = []
    for ln in scan:
        if not ln.strip():
            continue
        if STOP_HINTS.search(ln):
            break
        useful.append(ln)

    # 3) merge wrapped lines (desc then numeric tail)
    merged = _merge_wrapped_lines(useful)

    # 4) parse each merged line by tail-first logic + positional trim
    for ln in merged:
        if not _has_money(ln):
            continue

        qty, ext, unit, pos_qty, pos_ext, pos_unit, pos_first_money = _pick_tail_fields(ln)
        sku, pos_sku = _find_sku_pos(ln)
        uom, pos_uom = _find_last_uom_before(ln, pos_first_money)

        # Choose the earliest "tail token" start to trim description
        candidates = [p for p in (pos_qty, pos_ext, pos_unit, pos_sku, pos_uom) if p < 10**9]
        cut_at = min(candidates) if candidates else len(ln)

        desc = ln[:cut_at].strip()

        # Minimal acceptance: at least one of qty/ext/unit/sku
        if not (qty or ext or unit or sku):
            continue

        rows.append({
            "sku": sku,
            "qty": qty,
            "price": unit,     # unit price
            "ext": ext,        # extension (line total)
            "uom": uom,
            "description": desc,
        })

    return rows
