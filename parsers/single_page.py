from __future__ import annotations
import re
from typing import List, Dict, Tuple, Optional

HEADER_HINTS = re.compile(
    r"(?:^|\b)(part\s*number|item|sku|description|qty|quantity|unit\s*(?:price|cost)|extension|amount|total|ordered)(?:\b|$)",
    re.I,
)
STOP_HINTS = re.compile(
    r"\b(subtotal|total|tax|freight|grand\s*total|amount\s*due|receiving\s*hours|alt\s*contact)\b",
    re.I,
)

# Money MUST start with $
MONEY_RX = re.compile(r"\$\s*\d{1,3}(?:,\d{3})*(?:\.\d{2,4})?")

# standalone qty
NUM_TOKEN = re.compile(r"(?<![A-Za-z0-9])\d+(?:\.\d+)?(?![A-Za-z0-9])")

# Part numbers
SKU_STRICT_RX = re.compile(r"\b\d{3,5}-[A-Za-z0-9#]+\b")
SKU_LOOSE_RX  = re.compile(r"\b[A-Z0-9][A-Z0-9\-\./]{2,}\b")

# UOM patterns
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
    skus   = [(m.start(), m.end(), m.group(0)) for m in SKU_STRICT_RX.finditer(line)]
    if not skus:
        skus = [(m.start(), m.end(), m.group(0)) for m in SKU_LOOSE_RX.finditer(line)]
    nums   = [(m.start(), m.end(), m.group(0)) for m in NUM_TOKEN.finditer(line)]
    return monies, skus, nums

def _pick_tail_fields(line: str) -> Tuple[str, int, int, int, int]:
    monies, skus, nums = _extract_positions(line)
    pos_first_money = monies[0][0] if monies else 10**9
    pos_sku = skus[0][0] if skus else 10**9
    qty = ""
    pos_qty = 10**9
    for s, e, v in nums:
        if e <= pos_first_money:
            qty = v
            pos_qty = s
    u_positions = [m.start() for m in UOM_FUSED_RX.finditer(line) if m.start() < pos_first_money]
    u_positions += [m.start() for m in UOM_WORD_RX.finditer(line) if m.start() < pos_first_money]
    u_positions += [m.start() for m in UOM_GENERIC_RX.finditer(line) if m.start() < pos_first_money]
    pos_uom = max(u_positions) if u_positions else 10**9
    return qty, pos_qty, pos_first_money, pos_sku, pos_uom

def _strip_description(line: str, pos_qty: int, pos_first_money: int, pos_sku: int, pos_uom: int) -> str:
    candidates = [p for p in (pos_qty, pos_first_money, pos_sku, pos_uom) if p < 10**9]
    cut_at = min(candidates) if candidates else len(line)
    desc = line[:cut_at].strip()
    # FIX: use flags=re.I instead of inline (?i)
    desc = re.sub(r"\s+(?:sleeve|case|tray|pk|ea|each|cs|bx|bag|ct|carton|pallet)\w*\s*$", "", desc, flags=re.I)
    desc = re.sub(r"\s+[A-Za-z]{2,}\d{2,}[A-Za-z]*\s*$", "", desc)
    return desc.strip(" -–•\t")

def _find_sku(line: str) -> str:
    m = SKU_STRICT_RX.search(line)
    if m:
        return m.group(0)
    m2 = SKU_LOOSE_RX.search(line)
    return m2.group(0) if m2 else ""

def parse_single_page(full_text: str) -> List[Dict[str, str]]:
    lines = (full_text or "").splitlines()
    rows: List[Dict[str, str]] = []
    if not lines:
        return rows
    hidx = _find_header_index(lines)
    scan = lines[hidx + 1 :]
    useful: List[str] = []
    for ln in scan:
        if not (ln or "").strip():
            continue
        if STOP_HINTS.search(ln):
            break
        useful.append(ln)
    merged = _merge_wrapped_lines(useful)
    for ln in merged:
        if not _has_money(ln):
            continue
        qty, p_qty, p_money, p_sku, p_uom = _pick_tail_fields(ln)
        sku = _find_sku(ln)
        desc = _strip_description(ln, p_qty, p_money, p_sku, p_uom)
        if not (qty or sku or desc):
            continue
        rows.append({
            "sku": sku.strip(),
            "qty": qty.strip(),
            "description": desc.strip(),
        })
    return rows
