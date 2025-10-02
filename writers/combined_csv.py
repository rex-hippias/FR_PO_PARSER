# writers/combined_csv.py
from __future__ import annotations

import csv
import os
from typing import List, Dict

# Expected normalized keys incoming from run_agent:
FIELDNAMES = ["po_number", "file_name", "line_number", "sku", "qty", "price"]

def write_combined_csv(out_path: str, rows: List[Dict[str, str]]) -> str:
    """
    Write the combined CSV to `out_path` from in-memory rows.
    Returns the path written.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for r in rows:
            # ensure all expected fields exist
            w.writerow({k: (r.get(k, "") or "") for k in FIELDNAMES})

    return out_path
