#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Deterministic CSV generator for end-to-end input-formatter throughput benchmarks.

Emits 5 unquoted UINT64 columns: creationTS,key,v1,v2,v3
  - creationTS: event-time in ms, advances by 1 ms every `rows_per_ms` rows
                (so tumbling/sliding windows see many tuples per window).
  - key:        group key with cardinality `keys` (round-robin).
  - v1,v2,v3:   pseudo-random-but-deterministic payload values.

Integer columns keep per-field value parsing as cheap as possible, which
maximises the *visible* share of the input formatter (indexing) in the
end-to-end query time -- i.e. the best case for a faster CSV indexer.

Usage:
  gen_formatter_csv.py <output.csv> --target-mb 200 [--keys 64] [--rows-per-ms 50]
"""
import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a deterministic 5xUINT64 CSV for formatter benchmarks")
    parser.add_argument("output")
    parser.add_argument("--target-mb", type=int, default=200, help="approximate output size in MiB")
    parser.add_argument("--keys", type=int, default=64, help="group-key cardinality")
    parser.add_argument("--rows-per-ms", type=int, default=50, help="rows sharing the same creationTS")
    parser.add_argument("--base-ts", type=int, default=1740557983451, help="starting event timestamp (ms)")
    args = parser.parse_args()

    target_bytes = args.target_mb * (1 << 20)
    base_ts = args.base_ts

    written = 0
    row = 0
    # Buffer writes to keep generation fast (~hundreds of MB).
    buf = []
    buf_len = 0
    with open(args.output, "w", buffering=1 << 20) as out:
        while written < target_bytes:
            ts = base_ts + (row // args.rows_per_ms)
            key = row % args.keys
            # cheap deterministic LCG-ish payload, kept within ~7 digits
            v1 = (row * 2654435761) % 1000003
            v2 = (row * 40503 + 12345) % 1000003
            v3 = (row * 97 + key) % 1000003
            line = f"{ts},{key},{v1},{v2},{v3}\n"
            buf.append(line)
            buf_len += len(line)
            row += 1
            if buf_len >= (1 << 20):
                chunk = "".join(buf)
                out.write(chunk)
                written += buf_len
                buf.clear()
                buf_len = 0
        if buf:
            out.write("".join(buf))
            written += buf_len

    print(f"wrote {args.output}: {row} rows, ~{written / (1 << 20):.1f} MiB, key cardinality {args.keys}")


if __name__ == "__main__":
    main()
