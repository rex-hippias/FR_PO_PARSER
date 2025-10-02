# parsers/multi_page.py
from __future__ import annotations

from typing import List, Dict
from .single_page import parse_single_page

def parse_multi_page(pages: List[str]) -> List[Dict[str, str]]:
    """
    Parse a multi-page PO by parsing each page and concatenating rows.
    Uses the same line parser as single-page.
    """
    all_rows: List[Dict[str, str]] = []
    for page_text in pages:
        page_rows = parse_single_page(page_text)
        all_rows.extend(page_rows)
    return all_rows
