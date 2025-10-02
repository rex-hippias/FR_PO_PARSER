from __future__ import annotations
from typing import List, Dict
from .single_page import parse_single_page

def parse_multi_page(pages: List[str]) -> List[Dict[str, str]]:
    """
    Calls the single-page parser on each page and concatenates the results.
    """
    all_rows: List[Dict[str, str]] = []
    for page_text in pages:
        all_rows.extend(parse_single_page(page_text))
    return all_rows
