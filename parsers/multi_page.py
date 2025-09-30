from typing import List, Dict
from .single_page import parse_single_page

def parse_multi_page(pages: List[str]) -> List[Dict[str, str]]:
    """
    Baseline: concatenate pages and reuse single_page parser.
    Later we can improve header/column detection across page boundaries.
    """
    merged = "\n".join(pages)
    return parse_single_page(merged)