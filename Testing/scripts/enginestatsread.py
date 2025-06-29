#!/usr/bin/env python3
import sys
import os
import re
from datetime import datetime
from collections import defaultdict
import multiprocessing
import concurrent.futures
import time

# Pre-compile regex patterns for better performance
START_RE = re.compile(r'(?P<ts>[\d\-]+ [\d:\.]+) Task (?P<tid>\d+) for Pipeline (?P<p>\d+) of')
COMPLETE_RE = re.compile(r'(?P<ts>[\d\-]+ [\d:\.]+) Task (?P<tid>\d+) for Pipeline (?P<p>\d+) of.*Completed')
TUPLE_RE = re.compile(r'Number of Tuples: (?P<t>\d+)')

def parse_ts(raw):
    """Parse timestamp with high precision."""
    date, _, frac = raw.partition('.')
    frac_micro = (frac + '000000')[:6]  # Truncate to microseconds for datetime
    dt = datetime.fromisoformat(f'{date}.{frac_micro}')

    # For high precision calculations, extract seconds with full nanosecond precision
    time_str = raw.split()[1]  # Get time part: HH:MM:SS.NNNNNNNNN
    h, m, s = time_str.split(':')
    seconds = float(h) * 3600 + float(m) * 60 + float(s)

    return dt, seconds

def compute_stats(stats_path):
    """Process a single stats file and return the results."""
    try:
        starts_dt, starts_sec = {}, {}
        ends_dt, ends_sec = {}, {}
        tuples = {}

        with open(stats_path, 'r') as f:
            for line in f:
                if 'Started' in line:
                    m1, m2 = START_RE.search(line), TUPLE_RE.search(line)
                    if m1 and m2:
                        key = (int(m1.group('p')), int(m1.group('tid')))
                        dt, sec = parse_ts(m1.group('ts'))
                        starts_dt[key] = dt
                        starts_sec[key] = sec
                        tuples[key] = int(m2.group('t'))
                elif 'Completed' in line:
                    m = COMPLETE_RE.search(line)
                    if m:
                        key = (int(m.group('p')), int(m.group('tid')))
                        dt, sec = parse_ts(m.group('ts'))
                        ends_dt[key] = dt
                        ends_sec[key] = sec

        # Track skipped tasks
        skipped_by_pipeline = defaultdict(int)
        for key in list(starts_dt.keys()):
            if key not in ends_dt:
                skipped_by_pipeline[key[0]] += 1

        for key in list(ends_dt.keys()):
            if key not in starts_dt:
                skipped_by_pipeline[key[0]] += 1

        # Calculate durations using high-precision seconds
        durations = {}
        throughputs = {}
        for key in starts_dt.keys():
            if key in ends_dt:
                dur = ends_sec[key] - starts_sec[key]
                if dur > 0:
                    durations[key] = dur
                    throughputs[key] = tuples[key] / dur

        # Get wall clock times per pipeline
        pipeline_wall_times = {}
        for pid in set(p for p, _ in durations.keys()):
            pipeline_tasks = [(p, t) for (p, t) in durations.keys() if p == pid]
            if not pipeline_tasks:
                continue
            pipeline_starts = [starts_sec[k] for k in pipeline_tasks]
            pipeline_ends = [ends_sec[k] for k in pipeline_tasks]
            pipeline_wall_times[pid] = max(pipeline_ends) - min(pipeline_starts)

        # Calculate full-query time from matched tasks
        if durations:
            all_task_time = sum(durations.values())
            sts = [starts_sec[k] for k in durations]
            ets = [ends_sec[k] for k in durations]
            full_query_time = max(ets) - min(sts)
        else:
            all_task_time = full_query_time = 0.0

        # per-pipeline aggregates
        agg = {}
        for (pid, _), tp in throughputs.items():
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
            s['pct_time'] = s['sum_time'] / all_task_time * 100 if all_task_time else 0
            s['skipped'] = skipped_by_pipeline[pid]

            # Computational throughput (per compute time)
            s['comp_tp'] = s['sum_tuples'] / s['sum_time'] if s['sum_time'] > 0 else 0

            # Effective throughput (per wall clock time)
            wall_time = pipeline_wall_times.get(pid, 0)
            s['eff_tp'] = s['sum_tuples'] / wall_time if wall_time > 0 else 0

        total_skipped = sum(skipped_by_pipeline.values())
        return stats_path, all_task_time, full_query_time, agg, total_skipped

    except Exception as e:
        return stats_path, 0, 0, {}, f"Error: {str(e)}"

def main():
    if len(sys.argv) != 2:
        script = os.path.basename(__file__)
        print(f"Usage: python {script} DirPath")
        sys.exit(1)

    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"{directory} is not a directory")
        sys.exit(1)

    # Find all .stats files
    stats_files = []
    for filename in os.listdir(directory):
        if filename.endswith('.stats'):
            stats_files.append(os.path.join(directory, filename))

    if not stats_files:
        print(f"No .stats files found in {directory}")
        sys.exit(0)

    # Determine optimal number of workers
    num_workers = min(multiprocessing.cpu_count(), len(stats_files))
    print(f"Processing {len(stats_files)} files using {num_workers} CPU cores...")

    # Process files in parallel
    results = []
    start_time = time.time()
    completed = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {executor.submit(compute_stats, file_path): file_path for file_path in stats_files}

        # Simple progress indicator
        for future in concurrent.futures.as_completed(future_to_file):
            results.append(future.result())
            completed += 1
            print(f"Progress: {completed}/{len(stats_files)} files processed ({completed/len(stats_files)*100:.1f}%)")

    elapsed = time.time() - start_time
    print(f"Processing completed in {elapsed:.2f} seconds")

    # Write results to file
    results_path = os.path.join(directory, "results.txt")
    with open(results_path, 'w') as results_file:
        for file_path, total_time, full_time, pipelines, total_skipped in sorted(results):
            filename = os.path.basename(file_path)
            results_file.write(f"File: {filename}\n")

            if isinstance(total_skipped, str) and total_skipped.startswith("Error"):
                results_file.write(f"{total_skipped}\n\n")
                results_file.write("-"*50 + "\n\n")
                continue

            results_file.write(f"Total tasks time: {total_time:.2f}s\n")
            results_file.write(f"Full query duration: {full_time:.2f}s\n")
            results_file.write(f"Total skipped tasks: {total_skipped}\n\n")

            for pid in sorted(pipelines):
                p = pipelines[pid]
                results_file.write(
                    f"Pipeline {pid}: "
                    f"comp_tp={p['comp_tp']:.2f} tuples/s, "
                    f"eff_tp={p['eff_tp']:.2f} tuples/s, "
                    f"total_tuples={p['sum_tuples']}, "
                    f"time_pct={p['pct_time']:.2f}%, "
                    f"skipped_tasks={p['skipped']}\n"
                )

            results_file.write("\n" + "-"*50 + "\n\n")

    print(f"Results written to {results_path}")

if __name__ == "__main__":
    main()