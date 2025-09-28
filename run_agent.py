# run_agent.py
import os
import csv
import time
import argparse
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--parsed", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--logs", required=True)
    args = parser.parse_args()

    # Simulate some work
    time.sleep(float(os.getenv("AGENT_SLEEP_SECONDS", "1.0")))

    # Optionally force a failure for testing
    if os.getenv("FORCE_FAIL", "0") == "1":
        print("Forcing failure via FORCE_FAIL=1", file=sys.stderr)
        return 2

    os.makedirs(args.output, exist_ok=True)
    os.makedirs(args.logs, exist_ok=True)

    # Choose filename
    run_id = args.run_id
    alt_name = os.getenv("ALT_CSV_NAME")  # e.g., "results.csv" to test fallback
    if alt_name:
        csv_path = os.path.join(args.output, alt_name)
    else:
        csv_path = os.path.join(args.output, f"combined_{run_id}.csv")

    # Write a simple CSV
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["po_number", "line_number", "sku", "qty", "price"])
        w.writerow(["PO-STUB", "1", "SKU-STUB", "2", "5.00"])

    # Optionally write some logs
    with open(os.path.join(args.logs, "agent.stdout.txt"), "a") as f:
        f.write(f"run_agent: wrote CSV to {csv_path}\n")
    with open(os.path.join(args.logs, "agent.stderr.txt"), "a") as f:
        f.write("")

    return 0

if __name__ == "__main__":
    code = main()
    sys.exit(code)
