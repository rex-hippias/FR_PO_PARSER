from __future__ import annotations
import csv, os
from typing import List, Dict

# Exact header order you requested:
FIELDNAMES = [
    "Order Number",
    "Part Number",
    "Description",
    "Ordered",
    "Ship-To",
    "Delivery Date",
    "Source File",
    "Page",
]

def write_combined_csv(out_path: str, rows: List[Dict[str, str]]) -> str:
    """
    Expects rows already enriched by run_agent.py with keys:
      order_number, part_number, description, ordered, ship_to, delivery_date, source_file, page

    Fallbacks:
      - If 'part_number' missing but 'sku' present, use 'sku'
      - If 'ordered' missing but 'qty' present, use 'qty'
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for r in rows:
            part = (r.get("part_number") or r.get("sku") or "").strip()
            ordered = (r.get("ordered") or r.get("qty") or "").strip()
            w.writerow({
                "Order Number":  r.get("order_number", ""),
                "Part Number":   part,
                "Description":   r.get("description", ""),
                "Ordered":       ordered,
                "Ship-To":       r.get("ship_to", ""),
                "Delivery Date": r.get("delivery_date", ""),
                "Source File":   r.get("source_file", ""),
                "Page":          r.get("page", ""),
            })
    return out_path
