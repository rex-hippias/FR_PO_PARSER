# run_agent.py

import argparse
import sys
from pathlib import Path

from parsers.single_page import parse_single_page
from parsers.multi_page import parse_multi_page
from writers.combined_csv import write_combined_csv


def main():
    parser = argparse.ArgumentParser(description="Run PO Agent parser")
    parser.add_argument("--run-id", required=True, help="Run identifier")
    parser.add_argument("--input", required=True, help="Input directory with PDFs")
    parser.add_argument("--parsed", required=True, help="Directory for parsed CSVs")
    parser.add_argument("--output", required=True, help="Output directory for combined CSV")
    parser.add_argument("--logs", required=True, help="Logs directory")
    args = parser.parse_args()

    run_id = args.run_id
    input_dir = Path(args.input)
    parsed_dir = Path(args.parsed)
    output_dir = Path(args.output)
    logs_dir = Path(args.logs)

    # Ensure dirs exist
    parsed_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Parse each PDF
        for pdf_file in input_dir.glob("*.pdf"):
            if pdf_file.stat().st_size == 0:
                print(f"[worker] skipping empty PDF {pdf_file.name}")
                continue

            print(f"[worker] processing {pdf_file.name} ...")

            if "multi" in pdf_file.stem.lower():
                parse_multi_page(pdf_file, parsed_dir, run_id)
            else:
                parse_single_page(pdf_file, parsed_dir, run_id)

        # Combine into single CSV
        csv_path = write_combined_csv(parsed_dir, output_dir, run_id)
        print(f"[worker] combined CSV written: {csv_path}")

    except Exception as e:
        # Log error
        error_file = logs_dir / "agent.stderr.txt"
        with open(error_file, "w") as f:
            f.write(str(e))
        print(f"[worker] agent exited with error: {e}", file=sys.stderr)
        sys.exit(1)

    print("[worker] agent completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
