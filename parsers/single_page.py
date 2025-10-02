from __future__ import annotations
import re
from typing import List, Dict, Tuple, Optional

# --- Heuristics & patterns (tuned to your sample) ---
HEADER_HINTS = re.compile(
    r"(?:^|\b)(part\s*number|item|sku|description|qty|quantity|unit\s*(?:price|cost)|extension|amount|total|ordered)(?:\b|$)",
    re.I,
)
STOP_HINTS = re.compile(
    r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due|receiving\s*hours|alt\s*contact)\b",
    re.I,
)

# "$1,234.00" or "(1,234.00)"
MONEY_RX = re.compile(r"\$?\(?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2,4})?\)?")
# standalone quantity (won’t grab digits embedded in words like "Case500E")
NUM_TOKEN = re.compile(r"(?<![A-Za-z0-9])\d+(?:\.\d+)?(?![A-Za-z0-9])")

# Part number styles
SKU_STRICT_RX = re.compile(r"\b\d{3,5}-[A-Za-z0-9#]+\b")   # e.g., 0605-2507104
SKU_LOOSE_RX  = re.compile(r"\b[A-Z0-9][A-Z0-9\-\./]{2,}\b")

# UOM-like tokens, e.g., Case500E, CS500, PK12, etc.
UOM_RX = re.compile(r"\b(?:case|cs|pk|ea|each|bx|bag|ct|carton|pallet|tray|sleeve)[A-Za-z]*\d*[A-Za-z0-9]*\b", re.I)
UOM_GENERIC_RX = re.compile(r"\b[A-Za-z]{2,}\d{2,}[A-Za-z]*\b")

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
    Fits the sample where desc is on one line and numeric tail is on the next.
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
                # still no money → keep accumulating
                buf = ln2
            else:
                merged.append(ln2)
                buf = None

    if buf:
        merged.append(buf)
    return merged

def _extract_positions(line: str):
    monies = [(m.start(), m.end(), _clean_money(m.group(0))) for m in MONEY_RX.finditer(line)]
    # prefer strict SKU, fallback to loose
    skus   = [(m.start(), m.end(), m.group(0)) for m in SKU_STRICT_RX.finditer(line)]
    if not skus:
        skus = [(m.start(), m.end(), m.group(0)) for m in SKU_LOOSE_RX.finditer(line)]
    # UOMs
    uoms   = [(m.start(), m.end(), m.group(0)) for m in UOM_RX.finditer(line)]
    if not uoms:
        uoms = [(m.start(), m.end(), m.group(0)) for m in UOM_GENERIC_RX.finditer(line)]
    # numeric tokens (qty candidates)
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
    for m in (UOM_RX.finditer(line) or []):
        if m.end() <= cutoff:
            last = (m.group(0), m.start())
    # also consider generic UOM-ish tokens
    for m in (UOM_GENERIC_RX.finditer(line) or []):
        if m.end() <= cutoff and m.start() < last[1]:
            last = (m.group(0), m.start())
    return last

def _strip_description(line: str, pos_qty: int, pos_ext: int, pos_unit: int, pos_sku: int, pos_uom: int) -> str:
    """
    Trim description at the earliest tail token and also remove any lingering 'Case####' or UOM hash.
    """
    candidates = [p for p in (pos_qty, pos_ext, pos_unit, pos_sku, pos_uom) if p < 10**9]
    cut_at = min(candidates) if candidates else len(line)
    desc = line[:cut_at].strip()
    # scrub common UOM noise from the right end if it slipped into the desc
    desc = re.sub(r"\s+(?:Case|CS|PK|EA|Each|BX|Bag|CT|Carton|Pallet|Tray|Sleeve)\w*\s*$", "", desc, flags=re.I)
    desc = re.sub(r"\s+[A-Za-z]{2,}\d{2,}[A-Za-z]*\s*$", "", desc)  # generic “word+digits”
    return desc.strip(" -–•\t")

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    """
    Returns rows: dicts with keys:
      sku, description, qty, price (unit), ext (extension), uom
    """
    lines = full_text.splitlines()
    rows: List[Dict[str, str]] = []
    if not lines:
        return rows

    # 1) find header-ish line and scan below it
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

        qty, ext, unit, p_qty, p_ext, p_unit, p_first = _pick_tail_fields(ln)
        sku, p_sku = _find_sku_pos(ln)
        uom, p_uom = _find_last_uom_before(ln, p_first)

        desc = _strip_description(ln, p_qty, p_ext, p_unit, p_sku, p_uom)

        # accept if any meaningful fields present
        if not (qty or unit or ext or sku):
            continue

        rows.append({
            "sku": sku,
            "qty": qty,
            "price": unit,     # unit price (not used by your final CSV but retained)
            "ext": ext,        # extension
            "uom": uom,
            "description": desc,
        })

    return rows
