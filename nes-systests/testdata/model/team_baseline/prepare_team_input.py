#!/usr/bin/env python3
"""Builds the MODEL_INFERENCE input rows for the TEAM baseline systest.

The TEAM preprocessing happens outside the SavedModel, so this script performs it
and writes one CSV row per event:

    event_idx,event_id,waveforms,coords

`waveforms` is a [25, 3000, 3] and `coords` a [25, 3] float32 tensor, both
little-endian, row-major and base64-encoded, so the systest can decode them with
FROM_BASE64 into the two VARSIZED model inputs.

Preprocessing for a prediction at time t (seconds), with cutout = 100 * (5 + t):
  - stations stay in their station_idx order, channels in n, e, z order
  - per station and channel, subtract the mean of samples [0, cutout) and zero
    every sample from cutout onwards
  - stations whose P pick is later than cutout are zeroed, coords included
  - pad to 25 stations with zeros; with more than 25, keep the 25 earliest P picks
  - coords are the raw latitude, longitude and depth_km

Usage:
    prepare_team_input.py --stations stations_test.csv --waveforms waveforms_test.csv \
        --time 5 --events 0 31 34 --output team_input_t5.csv
"""

import argparse
import base64
import csv
from collections import defaultdict

import numpy as np

MAX_STATIONS = 25
SAMPLES = 3000
SAMPLING_RATE = 100


def read_stations(path, events):
    stations = defaultdict(list)
    with open(path, newline="") as file:
        for row in csv.DictReader(file):
            if int(row["event_idx"]) in events:
                stations[int(row["event_idx"])].append(row)
    return stations


def read_waveforms(path, events):
    waveforms = {}
    with open(path, newline="") as file:
        reader = csv.reader(file)
        next(reader)
        for event, station, sample, _timestamp, n, e, z in reader:
            if int(event) not in events:
                continue
            key = (int(event), int(station))
            if key not in waveforms:
                waveforms[key] = np.zeros((SAMPLES, 3), dtype=np.float32)
            waveforms[key][int(sample)] = (float(n), float(e), float(z))
    return waveforms


def prepare(event, stations, waveforms, time):
    cutout = int(round(SAMPLING_RATE * (5 + time)))
    rows = sorted(stations, key=lambda row: int(row["station_idx"]))
    if len(rows) > MAX_STATIONS:
        earliest = sorted(rows, key=lambda row: int(row["p_pick_sample"]))[:MAX_STATIONS]
        rows = sorted(earliest, key=lambda row: int(row["station_idx"]))

    event_waveforms = np.zeros((MAX_STATIONS, SAMPLES, 3), dtype=np.float32)
    event_coords = np.zeros((MAX_STATIONS, 3), dtype=np.float32)
    for slot, row in enumerate(rows):
        if int(row["p_pick_sample"]) > cutout:
            continue
        trace = waveforms[(event, int(row["station_idx"]))].copy()
        trace -= trace[:cutout].mean(axis=0, keepdims=True)
        trace[cutout:] = 0
        event_waveforms[slot] = trace
        event_coords[slot] = (float(row["latitude"]), float(row["longitude"]), float(row["depth_km"]))
    return event_waveforms, event_coords


def encode(tensor):
    return base64.b64encode(tensor.astype("<f4").tobytes()).decode("ascii")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stations", required=True)
    parser.add_argument("--waveforms", required=True)
    parser.add_argument("--time", type=float, required=True, help="prediction time t in seconds")
    parser.add_argument("--events", type=int, nargs="+", required=True, help="event_idx values to include")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    events = set(args.events)
    stations = read_stations(args.stations, events)
    waveforms = read_waveforms(args.waveforms, events)

    with open(args.output, "w", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        for event in args.events:
            event_waveforms, event_coords = prepare(event, stations[event], waveforms, args.time)
            event_id = stations[event][0]["event_id"]
            writer.writerow([event, event_id, encode(event_waveforms), encode(event_coords)])


if __name__ == "__main__":
    main()
