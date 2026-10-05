#!/usr/bin/env python3
"""Builds the inputs of the TEAM baseline systests.

The TEAM preprocessing happens outside the SavedModel, so this script performs it, for a prediction at time t
(seconds), with cutout = 100 * (5 + t):
  - stations stay in their station_idx order, channels in n, e, z order
  - per station and channel, subtract the mean of samples [0, cutout) and zero
    every sample from cutout onwards
  - stations whose P pick is later than cutout are zeroed, coords included
  - pad to 25 stations with zeros; with more than 25, keep the 25 earliest P picks
  - coords are the raw latitude, longitude and depth_km

It writes the result in one or both of two forms.

--output: finished model inputs, one CSV row per event:

    event_idx,event_id,waveforms,coords

`waveforms` is a [25, 3000, 3] and `coords` a [25, 3] float32 tensor, both
little-endian, row-major and base64-encoded, so the systest can decode them with
FROM_BASE64 into the two VARSIZED model inputs.

--stream-dir: the same data as sensor streams, one CSV file per station (named after its station_id), one row per
sample, in time order:

    event_idx,station_slot,sample_idx,timestamp_ms,n,e,z,latitude,longitude,depth_km

station_slot is the station's position in the event's tensors. timestamp_ms follows the test set's convention,
event_idx * 31000 + sample_idx * 10. Only data that exists at time t is written: samples before the cutout of
stations whose P wave has arrived. TENSOR_AGG's default value fills in the rest, which is TEAM's zero padding.

Usage:
    prepare_team_input.py --stations stations_test.csv --waveforms waveforms_test.csv \
        --time 5 --events 0 31 34 --output team_input_t5.csv
    prepare_team_input.py --stations stations_test.csv --waveforms waveforms_test.csv \
        --time 5 --events 3 26 47 --stream-dir stream_t5
"""

import argparse
import base64
import csv
import os
from collections import defaultdict

import numpy as np

MAX_STATIONS = 25
SAMPLES = 3000
SAMPLING_RATE = 100
EVENT_SPACING_MS = 31000


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
    """Returns the event's tensors, the cutout, and (slot, station row) for every station with data at time t."""
    cutout = int(round(SAMPLING_RATE * (5 + time)))
    rows = sorted(stations, key=lambda row: int(row["station_idx"]))
    if len(rows) > MAX_STATIONS:
        earliest = sorted(rows, key=lambda row: int(row["p_pick_sample"]))[:MAX_STATIONS]
        rows = sorted(earliest, key=lambda row: int(row["station_idx"]))

    event_waveforms = np.zeros((MAX_STATIONS, SAMPLES, 3), dtype=np.float32)
    event_coords = np.zeros((MAX_STATIONS, 3), dtype=np.float32)
    active = []
    for slot, row in enumerate(rows):
        if int(row["p_pick_sample"]) > cutout:
            continue
        trace = waveforms[(event, int(row["station_idx"]))].copy()
        trace -= trace[:cutout].mean(axis=0, keepdims=True)
        trace[cutout:] = 0
        event_waveforms[slot] = trace
        event_coords[slot] = (float(row["latitude"]), float(row["longitude"]), float(row["depth_km"]))
        active.append((slot, row))
    return event_waveforms, event_coords, cutout, active


def encode(tensor):
    return base64.b64encode(tensor.astype("<f4").tobytes()).decode("ascii")


def float_text(value):
    """Shortest text that parses back to exactly this float32."""
    return "0" if value == 0 else np.format_float_scientific(np.float32(value), unique=True, trim="-")


def write_station_streams(prepared_events, directory):
    """Appends every active station's samples before the cutout to the file of its station_id, event by event."""
    os.makedirs(directory, exist_ok=True)
    streams = defaultdict(list)
    for event, (event_waveforms, event_coords, cutout, active) in prepared_events:
        for slot, row in active:
            coords = [float_text(value) for value in event_coords[slot]]
            for sample in range(cutout):
                n, e, z = (float_text(value) for value in event_waveforms[slot, sample])
                timestamp = event * EVENT_SPACING_MS + sample * 1000 // SAMPLING_RATE
                streams[row["station_id"]].append([event, slot, sample, timestamp, n, e, z, *coords])
    for station_id, rows in streams.items():
        with open(os.path.join(directory, f"{station_id}.csv"), "w", newline="") as file:
            csv.writer(file, lineterminator="\n").writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stations", required=True)
    parser.add_argument("--waveforms", required=True)
    parser.add_argument("--time", type=float, required=True, help="prediction time t in seconds")
    parser.add_argument("--events", type=int, nargs="+", required=True, help="event_idx values to include")
    parser.add_argument("--output", help="CSV file for the finished model inputs")
    parser.add_argument("--stream-dir", help="directory for one CSV stream per station")
    args = parser.parse_args()
    if args.output is None and args.stream_dir is None:
        parser.error("at least one of --output and --stream-dir is required")

    events = sorted(set(args.events))
    stations = read_stations(args.stations, set(events))
    waveforms = read_waveforms(args.waveforms, set(events))
    prepared_events = [(event, prepare(event, stations[event], waveforms, args.time)) for event in events]

    if args.output is not None:
        with open(args.output, "w", newline="") as file:
            writer = csv.writer(file, lineterminator="\n")
            for event, (event_waveforms, event_coords, _cutout, _active) in prepared_events:
                event_id = stations[event][0]["event_id"]
                writer.writerow([event, event_id, encode(event_waveforms), encode(event_coords)])
    if args.stream_dir is not None:
        write_station_streams(prepared_events, args.stream_dir)


if __name__ == "__main__":
    main()
