#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Converts a Flink reference result (CSV from run_reference.sh) into NES systest rows.

Flink prints TIMESTAMP(3) values as 'YYYY-MM-DD HH:MM:SS[.fff]' (UTC, see ddl_files.sql) and quotes strings;
NES carries event time as epoch milliseconds and prints strings unquoted.

  to_nes_rows.py <flink.csv> [--columns 0,1,3]   print the rows, optionally reordered/projected
  to_nes_rows.py <flink.csv> --checksum         print '<rows> <checksum>' as the NES Checksum sink computes it

--string-columns lists VARSIZED columns whose values look like numbers (e.g. q13's side input value), so that the
checksum quotes them as NES does.

--float-columns lists columns that NES carries as FLOAT64 (e.g. Flink DECIMAL results such as q1's price):
Flink prints those as '184551' or '1.227616E+7', NES as '184551.0' or '12276160.0'.
"""

import argparse
import csv
import re
import sys
from datetime import datetime, timezone

TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,3})?$")
NUMBER = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")


def to_nes(value: str) -> str:
    if TIMESTAMP.match(value):
        parsed = datetime.strptime(value if "." in value else value + ".0", "%Y-%m-%d %H:%M:%S.%f")
        return str(int(parsed.replace(tzinfo=timezone.utc).timestamp() * 1000 + 0.5))
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("flink_csv")
    parser.add_argument("--columns", help="comma-separated column indices to emit, in order")
    parser.add_argument("--float-columns", default="", help="comma-separated indices (after --columns) of FLOAT64 columns")
    parser.add_argument(
        "--string-columns", default="", help="comma-separated indices (after --columns) of VARSIZED columns whose values look numeric"
    )
    parser.add_argument("--checksum", action="store_true")
    args = parser.parse_args()

    with open(args.flink_csv, newline="") as file:
        rows = [[to_nes(value) for value in row] for row in csv.reader(file)]
    if args.columns:
        indices = [int(index) for index in args.columns.split(",")]
        rows = [[row[index] for index in indices] for row in rows]
    for index in (int(index) for index in args.float_columns.split(",") if index):
        for row in rows:
            row[index] = repr(float(row[index]))
    if args.checksum:
        # The NES CSV formatter quotes VARSIZED values. Strings are recognized by content unless listed explicitly.
        string_columns = {int(index) for index in args.string_columns.split(",") if index}
        rows = [
            [value if NUMBER.match(value) and index not in string_columns else f'"{value}"' for index, value in enumerate(row)]
            for row in rows
        ]
    lines = sorted(",".join(row) for row in rows)

    if args.checksum:
        # Checksum::add: number of '\n' and the byte sum of the formatted CSV output.
        print(len(lines), sum(sum(line.encode()) + ord("\n") for line in lines))
    else:
        sys.stdout.write("".join(line + "\n" for line in lines))


if __name__ == "__main__":
    main()
