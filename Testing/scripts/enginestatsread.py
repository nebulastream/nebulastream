#!/usr/bin/env python3
import sys
import os
import json
import multiprocessing
import concurrent.futures
import time
import re
from collections import defaultdict

def compute_stats(trace_path):
    """Process a single trace file and return the results."""
    try:
        # Load the JSON trace file
        with open(trace_path, 'r') as f:
            trace_data = json.load(f)

        # Extract trace events
        events = trace_data.get('traceEvents', [])

        # Track task begin and end events
        tasks_begin = {}
        tasks_end = {}
        tuples = {}

        # Track pipeline start/end times
        pipeline_starts = defaultdict(list)
        pipeline_ends = defaultdict(list)

        # Parse events
        for event in events:
            # Skip events without required fields
            if not all(key in event for key in ['ph', 'ts', 'cat']):
                continue

            # Handle task begin events
            if event['ph'] == 'B' and event['cat'] == 'task' and 'args' in event:
                args = event['args']
                if all(k in args for k in ['pipeline_id', 'task_id']):
                    key = (args['pipeline_id'], args['task_id'])
                    tasks_begin[key] = event['ts']
                    if 'tuples' in args:
                        tuples[key] = args['tuples']
                    pipeline_starts[args['pipeline_id']].append(event['ts'])

            # Handle task end events
            elif event['ph'] == 'E' and event['cat'] == 'task' and 'args' in event:
                args = event['args']
                if all(k in args for k in ['pipeline_id', 'task_id']):
                    key = (args['pipeline_id'], args['task_id'])
                    tasks_end[key] = event['ts']
                    pipeline_ends[args['pipeline_id']].append(event['ts'])

        # Track skipped tasks
        skipped_by_pipeline = defaultdict(int)
        for key in list(tasks_begin.keys()):
            if key not in tasks_end:
                skipped_by_pipeline[key[0]] += 1

        for key in list(tasks_end.keys()):
            if key not in tasks_begin:
                skipped_by_pipeline[key[0]] += 1

        # Calculate durations using microseconds and convert to seconds
        durations = {}
        throughputs = {}
        for key in tasks_begin.keys():
            if key in tasks_end:
                # Convert from microseconds to seconds
                dur = (tasks_end[key] - tasks_begin[key]) / 1e6
                if dur > 0:
                    durations[key] = dur
                    if key in tuples:
                        throughputs[key] = tuples[key] / dur

        # Get wall clock times per pipeline
        pipeline_wall_times = {}
        for pid in set(p for p, _ in durations.keys()):
            if pid in pipeline_starts and pid in pipeline_ends:
                pipeline_wall_times[pid] = (max(pipeline_ends[pid]) - min(pipeline_starts[pid])) / 1e6

        # Calculate full-query time from matched tasks
        if durations:
            all_task_time = sum(durations.values())
            sts = [ts for k, ts in tasks_begin.items() if k in durations]
            ets = [ts for k, ts in tasks_end.items() if k in durations]
            full_query_time = (max(ets) - min(sts)) / 1e6
        else:
            all_task_time = full_query_time = 0.0

        # per-pipeline aggregates
        agg = {}
        for (pid, _), tp in throughputs.items():
            if (pid, _) not in tuples:
                continue

            stats = agg.setdefault(pid, {
                'sum_tp': 0, 'count': 0, 'sum_tuples': 0, 'sum_time': 0
            })
            stats['sum_tp'] += tp
            stats['count'] += 1
            stats['sum_tuples'] += tuples[(pid, _)]
            stats['sum_time'] += durations[(pid, _)]

        # finalize metrics and selectivity
        for pid, s in agg.items():
            s['avg_tp'] = s['sum_tp'] / s['count']
            s['time_pct'] = s['sum_time'] / all_task_time * 100 if all_task_time else 0
            s['skipped'] = skipped_by_pipeline[pid]

            # Computational throughput (per compute time)
            s['comp_tp'] = s['sum_tuples'] / s['sum_time'] if s['sum_time'] > 0 else 0

            # Effective throughput (per wall clock time)
            wall_time = pipeline_wall_times.get(pid, 0)
            s['eff_tp'] = s['sum_tuples'] / wall_time if wall_time > 0 else 0

        total_skipped = sum(skipped_by_pipeline.values())
        return trace_path, all_task_time, full_query_time, agg, total_skipped

    except Exception as e:
        return trace_path, 0, 0, {}, f"Error: {str(e)}"

def extract_metadata_from_filename(filename):
    """Extract metadata (layout, buffer size, threads, query) from filename."""
    metadata = {}

    # Extract layout
    if 'COLUMNAR_LAYOUT' in filename:
        metadata['layout'] = 'COLUMNAR_LAYOUT'
    elif 'ROW_LAYOUT' in filename:
        metadata['layout'] = 'ROW_LAYOUT'

    # Extract buffer size
    buffer_match = re.search(r'buffer(\d+)', filename)
    if buffer_match:
        metadata['buffer_size'] = buffer_match.group(1)

    # Extract thread count
    threads_match = re.search(r'threads(\d+)', filename)
    if threads_match:
        metadata['threads'] = threads_match.group(1)

    # Extract query number
    query_match = re.search(r'query(\d+)', filename)
    if query_match:
        metadata['query'] = query_match.group(1)

    return metadata

def main():
    # Get directory from command line
    if len(sys.argv) != 2:
        print("Usage: python enginestatsread.py <directory>")
        return

    directory = sys.argv[1]

    # Find all trace files in the directory
    trace_files = []
    for file in os.listdir(directory):
        if file.endswith('.json') and 'GoogleEventTrace' in file:
            trace_files.append(os.path.join(directory, file))

    # Process all trace files in parallel
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        results = list(executor.map(compute_stats, trace_files))

    # Process data for CSV output
    csv_data = []
    all_fields = set(['filename', 'layout', 'buffer_size', 'threads', 'query',
                      'total_tasks_time', 'full_query_duration', 'total_skipped_tasks'])

    for file_path, total_time, full_time, pipelines, total_skipped in sorted(results):
        if isinstance(total_skipped, str) and total_skipped.startswith("Error"):
            continue

        filename = os.path.basename(file_path)
        metadata = extract_metadata_from_filename(filename)

        row = {
            'filename': filename,
            'layout': metadata.get('layout', ''),
            'buffer_size': metadata.get('buffer_size', ''),
            'threads': metadata.get('threads', ''),
            'query': metadata.get('query', ''),
            'total_tasks_time': total_time,
            'full_query_duration': full_time,
            'total_skipped_tasks': total_skipped
        }

        # Add pipeline-specific data
        for pid in sorted(pipelines):
            p = pipelines[pid]
            # Add columns for this pipeline
            for metric in ['comp_tp', 'eff_tp', 'total_tuples', 'time_pct', 'skipped_tasks']:
                field_name = f'pipeline_{pid}_{metric}'
                row[field_name] = p.get(metric, 0)
                all_fields.add(field_name)

        csv_data.append(row)

    # Write the results to a text file
    with open(os.path.join(directory, 'results.txt'), 'w') as f:
        for file_path, total_time, full_time, pipelines, total_skipped in sorted(results):
            if isinstance(total_skipped, str) and total_skipped.startswith("Error"):
                f.write(f"File: {os.path.basename(file_path)}\n{total_skipped}\n\n")
                continue

            f.write(f"File: {os.path.basename(file_path)}\n")
            f.write(f"Total tasks time: {total_time:.2f}s\n")
            f.write(f"Full query duration: {full_time:.2f}s\n")
            f.write(f"Total skipped tasks: {total_skipped}\n\n")

            for pid in sorted(pipelines):
                p = pipelines[pid]
                f.write(f"Pipeline {pid}: comp_tp={p['comp_tp']:.2f} tuples/s, "
                        f"eff_tp={p['eff_tp']:.2f} tuples/s, "
                        f"total_tuples={p['sum_tuples']}, "
                        f"time_pct={p['time_pct']:.2f}%, "
                        f"skipped_tasks={p['skipped']}\n")
            f.write("\n--------------------------------------------------\n\n")

    # Write CSV with all possible fields
    csv_path = os.path.join(directory, os.path.basename(directory) + ".csv")
    import csv
    with open(csv_path, 'w', newline='') as csvfile:
        if csv_data:
            # Use the full collected field set for all rows
            fieldnames = sorted(list(all_fields))
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_data:
                # Ensure all fields exist (with empty values if necessary)
                for field in fieldnames:
                    if field not in row:
                        row[field] = 0  # Use 0 instead of empty for numeric fields
                writer.writerow(row)

    print(f"Results written to {os.path.join(directory, 'results.txt')}")
    print(f"CSV written to {csv_path}")

if __name__ == "__main__":
    main()