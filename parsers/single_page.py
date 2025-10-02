from __future__ import annotations
import re
from typing import List, Dict, Tuple, Optional

# --- Heuristics & patterns (tuned to vendor sample) ---
HEADER_HINTS = re.compile(
    r"(?:^|\b)(part\s*number|item|sku|description|qty|quantity|unit\s*(?:price|cost)|extension|amount|total|ordered)(?:\b|$)",
    re.I,
)
STOP_HINTS = re.compile(
    r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due|receiving\s*hours|alt\s*contact)\b",
    re.I,
)

# Money MUST start with $ to avoid matching "Case500E"
MONEY_RX = re.compile(r"\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{2,4})?")

# standalone quantity (won’t grab digits embedded in words like "Case500E")
NUM_TOKEN = re.compile(r"(?<![A-Za-z0-9])\d+(?:\.\d+)?(?![A-Za-z0-9])")

# Part number styles
SKU_STRICT_RX = re.compile(r"\b\d{3,5}-[A-Za-z0-9#]+\b")   # e.g., 0605-2507104
SKU_LOOSE_RX  = re.compile(r"\b[A-Z0-9][A-Z0-9\-\./]{2,}\b")

# UOM-like tokens (fused and standalone), e.g., Case500E, CS500, PK12, Sleeve...
UOM_FUSED_RX = re.compile(
    r"(?i)(?:sleeve|case|tray|pk|ea|each|cs|bx|bag|ct|carton|pallet)\w*\d+[A-Za-z]*"
)
UOM_WORD_RX = re.compile(
    r"(?i)\b(?:sleeve|case|tray|pk|ea|each|cs|bx|bag|ct|carton|pallet)\b"
)
UOM_GENERIC_RX = re.compile(r"\b[A-Za-z]{2,}\d{2,}[A-Za-z]*\b")

def _find_header_index(lines: List[str]) -> int:
    for i, ln in enumerate(lines):
        if HEADER_HINTS.search(ln):
            return i
    return 0

def _has_money(s: str) -> bool:
    return bool(MONEY_RX.search(s))

def _merge_wrapped_lines(lines: List[str]) -> List[str]:
    """
    Join description lines with the following line when the first line has no $ amount yet.
    Fits the sample where desc is on one line and numeric tail is on the next.
    """
    merged: List[str] = []
    buf: Optional[str] = None

    for raw in lines:
        ln = (raw or "").strip()
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
    monies = [(m.start(), m.end(), m.group(0)) for m in MONEY_RX.finditer(line)]
    # prefer strict SKU, fallback to loose
    skus   = [(m.start(), m.end(), m.group(0)) for m in SKU_STRICT_RX.finditer(line)]
    if not skus:
        skus = [(m.start(), m.end(), m.group(0)) for m in SKU_LOOSE_RX.finditer(line)]
    # numeric tokens (qty candidates)
    nums   = [(m.start(), m.end(), m.group(0)) for m in NUM_TOKEN.finditer(line)]
    return monies, skus, nums

def _pick_tail_fields(line: str) -> Tuple[str, int, int, int, int]:
    """
    Decide qty and the key tail positions for trimming.
    Returns: (qty, pos_qty, pos_first_money, pos_sku, pos_uom)
    """
    monies, skus, nums = _extract_positions(line)

    pos_first_money = monies[0][0] if monies else 10**9
    pos_sku = skus[0][0] if skus else 10**9

    # qty = last standalone number whose END is before the FIRST $ money
    qty = ""
    pos_qty = 10**9
    for s, e, v in nums:
        if e <= pos_first_money:
            qty = v
            pos_qty = s

    # last UOM-ish marker before first money (fused or word or generic)
    u_positions = [m.start() for m in UOM_FUSED_RX.finditer(line) if m.start() < pos_first_money]
    u_positions += [m.start() for m in UOM_WORD_RX.finditer(line) if m.start() < pos_first_money]
    u_positions += [m.start() for m in UOM_GENERIC_RX.finditer(line) if m.start() < pos_first_money]
    pos_uom = max(u_positions) if u_positions else 10**9

    return qty, pos_qty, pos_first_money, pos_sku, pos_uom

def _strip_description(line: str, pos_qty: int, pos_first_money: int, pos_sku: int, pos_uom: int) -> str:
    """
    Trim description at the earliest tail token and remove lingering UOM-ish fragments.
    """
    candidates = [p for p in (pos_qty, pos_first_money, pos_sku, pos_uom) if p < 10**9]
    cut_at = min(candidates) if candidates else len(line)
    desc = line[:cut_at].strip()
    # scrub UOM noise if any slipped through (right edge)
    desc = re.sub(r"\s+(?i)(?:sleeve|case|tray|pk|ea|each|cs|bx|bag|ct|carton|pallet)\w*\s*$", "", desc)
    desc = re.sub(r"\s+[A-Za-z]{2,}\d{2,}[A-Za-z]*\s*$", "", desc)  # generic word+digits
    return desc.strip(" -–•\t")

def _find_sku(line: str) -> str:
    m = SKU_STRICT_RX.search(line)
    if m:
        return m.group(0)
    m2 = SKU_LOOSE_RX.search(line)
    return m2.group(0) if m2 else ""

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    """
    Returns rows: dicts with keys:
      sku, description, qty
    (We keep it minimal for your final CSV mapping.)
    """
    lines = (full_text or "").splitlines()
    rows: List[Dict[str, str]] = []
    if not lines:
        return rows

    # 1) find header-ish line and scan below it
    hidx = _find_header_index(lines)
    scan = lines[hidx + 1 :]

    # 2) stop at totals/footer signals
    useful: List[str] = []
    for ln in scan:
        if not (ln or "").strip():
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

        qty, p_qty, p_money, p_sku, p_uom = _pick_tail_fields(ln)
        sku = _find_sku(ln)
        desc = _strip_description(ln, p_qty, p_money, p_sku, p_uom)

        # accept if any meaningful fields present
        if not (qty or sku or desc):
            continue

        rows.append({
            "sku": sku.strip(),
            "qty": qty.strip(),
            "description": desc.strip(),
        })

    return rows
