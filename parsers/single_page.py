import re
from typing import List, Dict, Tuple

# ---------- helpers ----------
MONEY_RX = re.compile(r"\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})?")
INT_RX   = re.compile(r"\d+")
QTY_RX   = re.compile(r"\d+(?:\.\d+)?")  # allow decimal qty

def clean_money(s: str) -> str:
    # Keep digits, comma, dot; strip $ and spaces
    s = s.strip()
    s = re.sub(r"[^\d\.,]", "", s)
    # normalize leading comma-only to digits
    return s

def clean_qty(s: str) -> str:
    # strip UOM like "EA", "CS", etc.
    m = QTY_RX.search(s)
    return m.group(0) if m else ""

def is_total_like(s: str) -> bool:
    s = s.lower()
    return any(k in s for k in ("subtotal", "total", "tax", "freight", "grand total"))

# ---------- header detection ----------
HEADER_ALIASES = {
    "line": {"line", "line#", "ln", "ln#", "ln no", "line no", "line number"},
    "item": {"item", "sku", "item#", "item no", "item number", "product", "code"},
    "desc": {"desc", "description", "item description", "prod desc"},
    "qty":  {"qty", "quantity", "order qty", "ordered", "ord qty"},
    "unit": {"unit", "unit price", "price", "u.price", "unit cost", "cost"},
    "ext":  {"extended", "ext", "ext price", "amount", "line total", "extended price"},
}

def normalize_token(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", t.lower()).strip()

def header_columns(header_line: str) -> List[str]:
    """
    Given a header line string, return a list of canonical columns in order,
    chosen from: line, item, desc, qty, unit, ext
    """
    # split on 2+ spaces to get tokens
    toks = [normalize_token(t) for t in re.split(r"\s{2,}", header_line) if t.strip()]
    cols: List[str] = []
    for t in toks:
        mapped = None
        for canon, aliases in HEADER_ALIASES.items():
            if any(a in t for a in aliases):
                mapped = canon
                break
        cols.append(mapped or t)  # keep unmapped tokens so positions align
    return cols

def find_header(lines: List[str]) -> Tuple[int, List[str]]:
    """
    Return (header_index, cols) or (-1, []) if not found.
    """
    for idx, raw in enumerate(lines):
        line = raw.strip()
        if not line:
            continue
        # require at least 3 header-like keywords present
        l = normalize_token(line)
        hits = sum(
            any(a in l for a in aliases)
            for aliases in HEADER_ALIASES.values()
        )
        if hits >= 3:
            cols = header_columns(line)
            return idx, cols
    return -1, []

# ---------- row parsing ----------
def parse_with_columns(lines: List[str], start_idx: int, cols: List[str]) -> List[Dict[str, str]]:
    """
    Parse rows under a detected header by splitting each line on 2+ spaces
    and mapping to the known columns. Stops when reaching totals section or blank block.
    """
    rows: List[Dict[str, str]] = []
    for raw in lines[start_idx+1:]:
        if not raw.strip():
            # allow sparse blank lines; continue unless we've already captured rows and see two blanks
            # (simple heuristic: break on first blank after we've started, but keep going initially)
            if rows:
                break
            else:
                continue
        if is_total_like(raw):
            break

        parts = [p.strip() for p in re.split(r"\s{2,}", raw.strip()) if p.strip()]
        # Some PDFs collapse multiple spaces; if we didn't split enough, skip to regex fallback later
        if len(parts) < 3:
            continue

        # Map according to detected columns
        # Build a dict with possible fields
        data: Dict[str, str] = {"line_number": "", "sku": "", "qty": "", "price": ""}

        # Determine indices by best-effort lookup
        def idx_of(canon: str) -> int:
            try:
                return cols.index(canon)
            except ValueError:
                return -1

        i_line = idx_of("line")
        i_item = idx_of("item")
        i_desc = idx_of("desc")
        i_qty  = idx_of("qty")
        i_unit = idx_of("unit")
        i_ext  = idx_of("ext")

        # Use ext price if unit missing; prefer unit for "price" field
        price_src = None
        if i_unit != -1 and i_unit < len(parts):
            price_src = parts[i_unit]
        elif i_ext != -1 and i_ext < len(parts):
            price_src = parts[i_ext]
        else:
            # fallback: last token that looks like money
            cand = [p for p in parts if MONEY_RX.fullmatch(p)]
            price_src = cand[-1] if cand else parts[-1]

        # Qty
        qty_src = ""
        if i_qty != -1 and i_qty < len(parts):
            qty_src = parts[i_qty]
        else:
            # guess: the token before price often is qty
            if len(parts) >= 2:
                qty_src = parts[-2]

        # Item/Desc
        item_src = ""
        if i_item != -1 and i_item < len(parts):
            item_src = parts[i_item]
        elif i_desc != -1 and i_desc < len(parts):
            item_src = parts[i_desc]
        else:
            # everything between (line?) and (qty/price) could be description
            # heuristic: join middle tokens excluding first and last 1–2
            mid = parts[1:-2] if len(parts) > 3 else parts[1:-1]
            item_src = " ".join(mid).strip()

        # Line number
        line_src = ""
        if i_line != -1 and i_line < len(parts):
            line_src = parts[i_line]
        else:
            # often first token
            line_src = parts[0]

        data["line_number"] = re.sub(r"[^\d]", "", line_src)[:6]
        data["sku"] = item_src
        data["qty"] = clean_qty(qty_src)
        data["price"] = clean_money(price_src)

        # sanity: need at least qty or price to consider it a line
        if data["qty"] or data["price"]:
            rows.append(data)

    return rows

# ---------- regex fallback (from your earlier version, a bit looser) ----------
LINE_PATTERNS = [
    # line  desc/sku         qty    price [ext optional]
    re.compile(r"^\s*(\d{1,4})\s+([^\d].*?)\s+(\d+(?:\.\d+)?)\s+(\$?\d[\d,]*\.\d{2})(?:\s+\$?\d[\d,]*\.\d{2})?\s*$"),
    # line  SKU               qty    price
    re.compile(r"^\s*(\d{1,4})\s+([A-Z0-9][A-Z0-9\-_\/]+)\s+(\d+(?:\.\d+)?)\s+(\$?\d[\d,]*\.\d{2})\s*$"),
    # alt ordering: line qty desc price
    re.compile(r"^\s*(\d{1,4})\s+(\d+(?:\.\d+)?)\s+([^\d].*?)\s+(\$?\d[\d,]*\.\d{2})\s*$"),
]

def parse_by_regex(all_lines: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for raw in all_lines:
        line = raw.strip()
        if not line or is_total_like(line):
            continue
        for pat in LINE_PATTERNS:
            m = pat.match(line)
            if m:
                ln, item, qty, price = m.group(1, 2, 3, 4)
                rows.append({
                    "line_number": re.sub(r"[^\d]", "", ln),
                    "sku": item.strip(),
                    "qty": clean_qty(qty),
                    "price": clean_money(price),
                })
                break
    return rows

# ---------- main entry ----------
def parse_single_page(text: str) -> List[Dict[str, str]]:
    """
    Attempts header-aware parsing first, then falls back to regex scanning.
    """
    lines = text.splitlines()
    h_idx, cols = find_header(lines)
    if h_idx != -1:
        rows = parse_with_columns(lines, h_idx, cols)
        if rows:
            return rows
    # fallback
    return parse_by_regex(lines)
