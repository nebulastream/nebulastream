#!/usr/bin/env python3
"""Reference oracle for the IntervalJoinLarge system test.

This script is the independent oracle used to derive the expected ChecksumSink
result for a streaming interval join. It

  1. deterministically generates a left and a right input stream (so the inputs
     are reproducible and can be regenerated at any scale),
  2. computes the inner interval join in plain Python, and
  3. writes the joined tuples as CSV in exactly the byte layout NebulaStream's
     sinks emit, so that running the `checksum` tool (ChecksumStarter) over this
     file yields the same `count,checksum` pair that NebulaStream's ChecksumSink
     produces for the equivalent query.

Interval-join semantics (matching NebulaStream):
  * inner join on the key field, closed interval predicate
        left.ts + lower <= right.ts <= left.ts + upper
  * the reported window is the W-aligned slice that contains the LEFT tuple's
    timestamp, where W = upper - lower:
        start = (left.ts // W) * W,  end = start + W
  * output column order is: start, end, <left fields...>, <right fields...>

Usage:
  interval_join_checksum.py gen   --rows N --keys K --step S --left L.csv --right R.csv
  interval_join_checksum.py join  --left L.csv --right R.csv --lower LO --upper HI --out O.csv
"""
import argparse
import csv


def gen(rows, keys, step, left_path, right_path):
    """Write `rows` left and right tuples each: (id, value, timestamp).

    Left tuple i  : id = i % keys, value = i + 1,          ts = i * step
    Right tuple j : id = j % keys, value = 1_000_000 + j,  ts = j * step + step // 2
    Values are unique per stream so the byte checksum stays sensitive to every
    individual joined pair.
    """
    with open(left_path, "w", newline="") as f:
        w = csv.writer(f)
        for i in range(rows):
            w.writerow([i % keys, i + 1, i * step])
    with open(right_path, "w", newline="") as f:
        w = csv.writer(f)
        for j in range(rows):
            w.writerow([j % keys, 1_000_000 + j, j * step + step // 2])


def read_stream(path):
    out = []
    with open(path, newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            out.append((int(row[0]), int(row[1]), int(row[2])))
    return out


def join(left_path, right_path, lower, upper, out_path):
    """Compute the inner interval join and write the result in NebulaStream layout."""
    assert lower < upper, "interval join requires lower < upper"
    width = upper - lower
    left = read_stream(left_path)
    # Bucket right tuples by key for a cheaper inner loop.
    right_by_key = {}
    for rid, rval, rts in read_stream(right_path):
        right_by_key.setdefault(rid, []).append((rid, rval, rts))

    count = 0
    with open(out_path, "w", newline="") as f:
        for lid, lval, lts in left:
            start = (lts // width) * width
            end = start + width
            for rid, rval, rts in right_by_key.get(lid, ()):
                if lts + lower <= rts <= lts + upper:
                    # NebulaStream emits "\n"-terminated, comma-separated rows.
                    f.write(f"{start},{end},{lid},{lval},{lts},{rid},{rval},{rts}\n")
                    count += 1
    return count


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("gen", help="generate deterministic left/right input streams")
    g.add_argument("--rows", type=int, required=True)
    g.add_argument("--keys", type=int, default=10)
    g.add_argument("--step", type=int, default=10)
    g.add_argument("--left", required=True)
    g.add_argument("--right", required=True)

    j = sub.add_parser("join", help="compute the interval join into NebulaStream CSV layout")
    j.add_argument("--left", required=True)
    j.add_argument("--right", required=True)
    j.add_argument("--lower", type=int, required=True)
    j.add_argument("--upper", type=int, required=True)
    j.add_argument("--out", required=True)

    args = p.parse_args()
    if args.cmd == "gen":
        gen(args.rows, args.keys, args.step, args.left, args.right)
    else:
        n = join(args.left, args.right, args.lower, args.upper, args.out)
        print(f"wrote {n} joined rows to {args.out}")


if __name__ == "__main__":
    main()
