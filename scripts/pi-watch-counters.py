#!/usr/bin/env python3
"""
Watch the coffee machine's raw counters and print only what CHANGES.

Reads the 43-field CSV stream straight off the relay (or the mock server) and
prints a line whenever any counter moves. Deliberately bypasses NES and MQTT:
when a real-machine test disagrees with the dashboard, this says whether the
machine or the pipeline is at fault.

It also exposes fields the queries do not publish. `total_water_quantity_ticks`
is the one that matters — the water query intentionally omits it (it is the
parent total, not a fourth sibling), but it is exactly the field you need to
calibrate ml-per-tick against a measured volume.

Usage:
  # against the relay (which is fed by the ssh tunnel)
  python3 scripts/pi-watch-counters.py --port 2222

  # calibration mode: totals only, reset the deltas with a keypress
  python3 scripts/pi-watch-counters.py --port 2222 --calibrate

Ctrl-C prints a summary of everything that moved during the session.
"""

import argparse
import csv
import io
import socket
import sys
import time
from datetime import datetime

# Field order IS the wire contract. Must stay identical to FIELDS in
# scripts/coffe-server-pi-schema.py and to the `logical:` block in every query.
FIELDS = [
    "ts_ms",
    "total_water_quantity_ticks", "total_water_products_ticks",
    "total_water_rinse_ticks", "total_water_cleaning_ticks",
    "rfu", "total_coffee_bean_quantity_g",
    "total_coffee_beans_left_rear_g", "total_coffee_beans_right_front_g",
    "total_coffee_powder_chute_g", "cleaning_counter",
    "brewcycles_left", "brewcycles_right",
    "cycles_rinse_clean_left", "cycles_rinse_clean_right",
    "time_milk_cs", "heating_time_boiler_left_cs", "heating_time_boiler_right_cs",
    "time_hot_water_cs", "time_steam_left_cs", "time_steam_right_cs",
    "heat_time_steam_boiler_1_cs", "heat_time_steam_boiler_2_cs",
    "time_air_pump_cs", "milk_cycles_left", "milk_cycles_right",
    "reset_date_day", "reset_date_month", "reset_date_year",
    "rfu2", "reset_time_second", "reset_time_minute", "reset_time_hour", "rfu3",
    "time_milk_steam_left_cs", "time_milk_steam_right_cs",
    "total_coffee_beans_grinder_adjustment_g", "absolute_counter",
    "time_powder_cs", "total_powder_quantity_left_g", "total_powder_quantity_right_g",
    "total_coffee_beans_3_g", "total_coffee_beans_4_g",
]

# Fields that are permanently zero on the real machine. Hidden by default so a
# change in one is conspicuous rather than lost in noise -- if one of these ever
# moves, an assumption in the queries and the dashboard is wrong.
DEAD = {
    "rfu", "rfu2", "rfu3", "total_coffee_powder_chute_g", "brewcycles_right",
    "cycles_rinse_clean_right", "heating_time_boiler_left_cs",
    "heating_time_boiler_right_cs", "time_steam_left_cs", "time_steam_right_cs",
    "heat_time_steam_boiler_1_cs", "heat_time_steam_boiler_2_cs",
    "milk_cycles_right", "time_milk_steam_right_cs", "time_powder_cs",
    "total_powder_quantity_left_g", "total_powder_quantity_right_g",
    "total_coffee_beans_4_g",
}

# Static machine metadata -- carries the counter reset stamp, not telemetry.
STATIC = {
    "reset_date_day", "reset_date_month", "reset_date_year",
    "reset_time_second", "reset_time_minute", "reset_time_hour",
}


def parse_row(line: str):
    values = next(csv.reader(io.StringIO(line)))
    if len(values) != len(FIELDS):
        return None
    try:
        return {name: int(v) for name, v in zip(FIELDS, values)}
    except ValueError:
        return None


def check_identities(row: dict) -> list[str]:
    """The two structural identities that must hold in real data."""
    problems = []
    beans = (row["total_coffee_beans_left_rear_g"]
             + row["total_coffee_beans_right_front_g"]
             + row["total_coffee_beans_3_g"]
             + row["total_coffee_beans_4_g"]
             + row["total_coffee_beans_grinder_adjustment_g"])
    if beans != row["total_coffee_bean_quantity_g"]:
        problems.append(
            f"bean identity off by {row['total_coffee_bean_quantity_g'] - beans} g")
    water = (row["total_water_products_ticks"] + row["total_water_rinse_ticks"]
             + row["total_water_cleaning_ticks"])
    # A CONSTANT offset here is expected (calibration artifact, ~1500 ticks).
    # Only report it so the operator can confirm it is not growing.
    problems.append(
        f"water offset {row['total_water_quantity_ticks'] - water} (should be constant)")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description="Print coffee counter changes")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2222,
                    help="relay or mock server port (default: 2222)")
    ap.add_argument("--show-dead", action="store_true",
                    help="also report the 18 permanently-zero fields")
    ap.add_argument("--calibrate", action="store_true",
                    help="print running totals for the ml-per-tick measurement")
    ap.add_argument("--seconds", type=float, default=0.0, metavar="N",
                    help="stop after N seconds and print the summary. Use this "
                         "for the calibration run so the measurement window is "
                         "exact instead of however long it took to hit Ctrl-C")
    args = ap.parse_args()

    print(f"connecting to {args.host}:{args.port} ...", file=sys.stderr)
    sock = socket.create_connection((args.host, args.port), timeout=30)
    print("connected. waiting for a counter to move (Ctrl-C for summary)\n",
          file=sys.stderr)

    prev = None
    session_start = None
    buf = b""
    started = time.time()

    deadline = started + args.seconds if args.seconds else None

    try:
        while True:
            if deadline and time.time() >= deadline:
                print(f"\nreached --seconds {args.seconds:g} limit")
                break
            if deadline:
                sock.settimeout(max(0.1, deadline - time.time()))
            chunk = sock.recv(4096)
            if not chunk:
                print("!! upstream closed the connection", file=sys.stderr)
                break
            buf += chunk
            while b"\n" in buf:
                raw, buf = buf.split(b"\n", 1)
                row = parse_row(raw.decode(errors="replace").strip())
                if row is None:
                    continue

                if all(row[f] == 0 for f in FIELDS if f != "ts_ms"):
                    print(f"{datetime.now():%H:%M:%S}  *** ALL-ZERO DROPOUT ROW "
                          f"(dashboard must ignore this) ***")
                    continue

                if prev is None:
                    prev = session_start = row
                    print(f"{datetime.now():%H:%M:%S}  baseline taken")
                    for p in check_identities(row):
                        print(f"           {p}")
                    continue

                changed = {
                    f: row[f] - prev[f]
                    for f in FIELDS
                    if f != "ts_ms" and row[f] != prev[f]
                    and (args.show_dead or f not in DEAD)
                    and f not in STATIC
                }
                if any(row[f] < prev[f] for f in FIELDS if f != "ts_ms"):
                    print(f"{datetime.now():%H:%M:%S}  *** COUNTER RESET "
                          f"(dashboard must re-baseline) ***")
                    session_start = row

                if changed:
                    parts = " ".join(f"{f}+{d}" if d > 0 else f"{f}{d}"
                                     for f, d in sorted(changed.items()))
                    print(f"{datetime.now():%H:%M:%S}  {parts}")
                    dead_moved = [f for f in changed if f in DEAD]
                    if dead_moved:
                        print(f"           !! a supposedly-dead field moved: "
                              f"{', '.join(dead_moved)}")

                if args.calibrate and session_start is not None:
                    print(f"           TOTAL water ticks this session: "
                          f"{row['total_water_quantity_ticks'] - session_start['total_water_quantity_ticks']}")

                prev = row
    except KeyboardInterrupt:
        print()
    except socket.timeout:
        print(f"\nreached --seconds {args.seconds:g} limit")
    finally:
        sock.close()

    if prev is not None and session_start is not None:
        print("\n=== session summary "
              f"({time.time() - started:.0f}s) ===")
        for f in FIELDS:
            if f == "ts_ms" or f in STATIC:
                continue
            d = prev[f] - session_start[f]
            if d:
                print(f"  {f:45} {d:+}")
        water = prev["total_water_quantity_ticks"] - session_start["total_water_quantity_ticks"]
        products = prev["absolute_counter"] - session_start["absolute_counter"]
        if products:
            print(f"\n  {products} product(s), {water} water ticks "
                  f"-> {water / products:.0f} ticks/product")
        if water:
            print(f"  If you dispensed a known volume V ml, "
                  f"ML_PER_TICK = V / {water}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
