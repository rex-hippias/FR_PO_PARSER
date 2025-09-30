import csv, os
from typing import List, Dict

FIELDS = ["po_number","file_name","line_number","sku","qty","price"]

def write_combined(rows: List[Dict[str,str]], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k,"") for k in FIELDS})