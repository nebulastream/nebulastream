#!/usr/bin/env python3
"""Adds a padding field to each row of a CSV file.

Takes an existing CSV (e.g., Nexmark BID dataset) and appends a VARSIZED
padding column with N bytes of filler text to each data row. This allows
benchmarking the effect of tuple size on synchronization overhead.

Usage:
    python3 add_csv_padding.py --input bid.csv --output bid_padded.csv --padding-size 1024
    python3 add_csv_padding.py --input bid.csv --output bid_padded.csv --padding-size 1024 --has-header
"""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="Add a padding field to each row of a CSV file.")
    parser.add_argument("--input", required=True, help="Path to the input CSV file")
    parser.add_argument("--output", required=True, help="Path to the output CSV file")
    parser.add_argument("--padding-size", type=int, required=True, help="Number of padding bytes per row")
    parser.add_argument("--has-header", action="store_true", help="Treat the first line as a header row")
    parser.add_argument("--pad-char", default="x", help="Character used for padding (default: x)")
    args = parser.parse_args()

    if args.padding_size < 0:
        print("Error: --padding-size must be non-negative", file=sys.stderr)
        sys.exit(1)

    padding_data = args.pad_char * args.padding_size

    with open(args.input, "r") as infile, open(args.output, "w") as outfile:
        for i, line in enumerate(infile):
            stripped = line.rstrip("\n\r")
            if i == 0 and args.has_header:
                outfile.write(f"{stripped},padding\n")
            else:
                outfile.write(f"{stripped},{padding_data}\n")


if __name__ == "__main__":
    main()
