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
    Input rows must already contain keys:
      order_number, part_number, description, ordered, ship_to, delivery_date, source_file, page
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for r in rows:
            w.writerow({
                "Order Number":  r.get("order_number", ""),
                "Part Number":   r.get("part_number", ""),
                "Description":   r.get("description", ""),
                "Ordered":       r.get("ordered", ""),
                "Ship-To":       r.get("ship_to", ""),
                "Delivery Date": r.get("delivery_date", ""),
                "Source File":   r.get("source_file", ""),
                "Page":          r.get("page", ""),
            })
    return out_path
