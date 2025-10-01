# writers/combined_csv.py

import csv
from pathlib import Path


def write_combined_csv(parsed_dir: Path, output_dir: Path, run_id: str) -> str:
    """
    Combine all parsed CSV files into a single combined CSV for this run.
    Returns the path to the combined CSV file.
    """

    # Ensure output dir exists
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"combined_{run_id}.csv"

    fieldnames = ["PO Number", "Line", "Description", "Quantity", "Unit"]
    rows = []

    # Collect rows from each CSV in parsed_dir
    for csv_file in parsed_dir.glob("*.csv"):
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

    # Write combined CSV
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return str(output_file)
            
