import re
from typing import List, Dict

def parse_single_page(text: str) -> List[Dict[str, str]]:
    LINE_PATTERNS = [
        # line  SKU/desc  qty  price
        re.compile(r"^\s*(\d{1,4})\s+([^\d].*?)\s+(\d{1,6})\s+(\$?\d[\d,]*\.\d{2})\s*$"),
        # line  SKU (code-like)  qty  price
        re.compile(r"^\s*(\d{1,4})\s+([A-Z0-9][A-Z0-9\-_\/]+)\s+(\d{1,6})\s+(\$?\d[\d,]*\.\d{2})\s*$"),
    ]

    rows: List[Dict[str, str]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        for pat in LINE_PATTERNS:
            m = pat.match(line)
            if m:
                rows.append({
                    "line_number": m.group(1),
                    "sku": m.group(2).strip(),
                    "qty": re.sub(r"[^\d]", "", m.group(3)),
                    "price": re.sub(r"[^\d\.,]", "", m.group(4)),
                })
                break
    return rows