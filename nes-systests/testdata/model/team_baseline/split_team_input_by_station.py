#!/usr/bin/env python3
"""Splits preprocessed TEAM events into one stream per station slot, for ARRAY_AGG-based tensor assembly.

Reads files written by prepare_team_input.py (event_idx,event_id,waveforms,coords with base64 float32 tensors
waveforms [25, 3000, 3] and coords [25, 3]) and writes one CSV per station slot:

    ts,value,coord

Every row carries one float of the station's waveform. `coord` holds latitude, longitude and depth_km in the
station's first three rows and is empty (NULL) everywhere else, so ARRAY_AGG(coord) skips it.

The timestamps are synthetic: they encode each value's position in the model's input tensors,

    ts = event * EVENT_SPACING + station * 9000 + sample * 3 + channel

so ARRAY_AGG, which orders by timestamp, reproduces the row-major [25, 3000, 3] layout. Every station slot covers
its own timestamp range, which keeps the pages of different sources disjoint, and a tumbling window of
EVENT_SPACING ms holds exactly one event. Events are numbered in the order of the input files.

Usage:
    split_team_input_by_station.py --inputs team_input_t5_event3.csv team_input_t5_event26.csv \
        --output-dir stations_t5
"""

import argparse
import base64
import csv
import os

import numpy as np

STATIONS = 25
VALUES_PER_STATION = 3000 * 3
EVENT_SPACING = 250000


def float_text(value):
    """Shortest text that parses back to exactly this float32."""
    return "0" if value == 0 else np.format_float_scientific(np.float32(value), unique=True, trim="-")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--inputs", nargs="+", required=True, help="files written by prepare_team_input.py")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    csv.field_size_limit(1 << 30)
    events = []
    for path in args.inputs:
        with open(path, newline="") as file:
            for _event_idx, _event_id, waveforms, coords in csv.reader(file):
                events.append(
                    (
                        np.frombuffer(base64.b64decode(waveforms), "<f4").reshape(STATIONS, VALUES_PER_STATION),
                        np.frombuffer(base64.b64decode(coords), "<f4").reshape(STATIONS, 3),
                    )
                )

    os.makedirs(args.output_dir, exist_ok=True)
    for station in range(STATIONS):
        with open(os.path.join(args.output_dir, f"station_{station:02d}.csv"), "w", newline="") as file:
            writer = csv.writer(file, lineterminator="\n")
            for event, (waveforms, coords) in enumerate(events):
                first = event * EVENT_SPACING + station * VALUES_PER_STATION
                for offset, value in enumerate(waveforms[station]):
                    coord = float_text(coords[station][offset]) if offset < 3 else ""
                    writer.writerow([first + offset, float_text(value), coord])


if __name__ == "__main__":
    main()
