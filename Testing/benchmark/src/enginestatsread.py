#!/usr/bin/env python3
import sys
import os
import csv
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict
from run_benchmark import get_config_from_file
from pathlib import Path
import concurrent
import multiprocessing
import concurrent.futures
import json
import re

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

            # Mean latency (total time divided by task count)
            s['mean_latency'] = s['sum_time'] / s['count'] if s['count'] > 0 else 0

        total_skipped = sum(skipped_by_pipeline.values())
        return trace_path, all_task_time, full_query_time, agg, total_skipped

    except Exception as e:
        return trace_path, 0, 0, {}, f"Error: {str(e)}"


def process_csv(trace_paths, window_size=10):
    """ Process a single CSV file using pandas and return aggregated results."""
    try:

        # Read the CSV files into a pandas DataFrame
        #dfs = [pd.read_csv(f) for f in trace_paths]

        # Load the JSON trace file
        """dfs = []
        for trace_data in trace_paths:
            # Support both {"traceEvents": [...]} and a top-level list of events
            if isinstance(trace_data, dict):
                events = trace_data.get('traceEvents', [])
            elif isinstance(trace_data, list):
                events = trace_data
            else:
                events = []

            if not events:
                continue

            df = pd.DataFrame(events)
            if df.empty:
                continue

            # Ensure required columns exist
            for col in ['cat', 'ph', 'args', 'ts']:
                if col not in df.columns:
                    df[col] = None

            # Keep only task events with phase B or E and non-null args
            df = df[(df['cat'] == 'task') & (df['ph'].isin(['B', 'E'])) & (df['args'].notnull())].copy()
            if df.empty:
                continue


            #set tuples = 0 if missing or ph = B
            new_df = pd.DataFrame()
            new_df['p_id'] = df['args'].apply(lambda x: x.get('pipeline_id', -1)).astype(int)
            new_df['t_id'] = df['args'].apply(lambda x: x.get('task_id', -1)).astype(int)
            new_df['ph'] = df['ph'].astype(str)
            # Normalize timestamps to int and make them reasonable (keep lower digits)
            new_df['ts'] = pd.to_numeric(df['ts'], errors='coerce').fillna(0).astype(int) % (10**9)  # Ignore the first 9 digits of the timestamp
           # tuples only meaningful on begin events ('B'), default to 0 otherwise
            def extract_tuples(row):
                args = row['args']
                if isinstance(args, dict) and row['ph'] == 'B':
                    return int(args.get('tuples', 0) or 0)
                return 0
            new_df['tuples'] = df.apply(extract_tuples, axis=1)

            dfs.append(new_df)

        # If no valid data, return early
        if not dfs:
            print("No valid task events found in provided trace files.")
            return None, None, None, None, {}"""


        dfs = []
        for trace_file in trace_paths:
            df = []
            with open(trace_file, 'r') as f:
                trace_data = json.load(f)

            # Extract trace events
            events  = trace_data.get('traceEvents', [])

            rows = []
            for event in events:
                if event.get('ph') in ['B', 'E'] and event.get('cat') == 'task' and 'args' in event:

                    args = event['args']
                    # Ignore the first 9 digits of the timestamp
                    p_id = int(args.get('pipeline_id', -1))
                    t_id = int(args.get('task_id', -1))
                    ph = event.get('ph', '')
                    #reduced_ts = (event['ts'] % 1_000_000_000)
                    ts = int(event['ts'])

                    tuples = int(args.get('tuples', 0) if event['ph'] == 'B' else 0)
                    rows.append({
                        'p_id': p_id,
                         't_id': t_id,
                         'ph': ph,
                         'ts': ts,
                         'tuples': tuples
                    })
            if not rows:
                continue
            df =    pd.DataFrame(rows)

            min_timestamp = df['ts'].min()
            df['ts'] = (df['ts']  -  min_timestamp) % 1_000_000_000

            #print(df.info())
            #print(df.head())
            dfs.append(df)
        print(f"Loaded {len(dfs)} trace files for processing.")
        # concatenate over all number of runs
        df = pd.concat(dfs, keys=range(len(dfs)), names=['run_id']).reset_index(level=0).reset_index(drop=True)
        print(df.info())
        print(df.head())
        metadata = extract_metadata_from_filename(trace_paths[0]) #all files of same run have same metadata

        # Convert columns to appropriate types
        df['run_id'] = df['run_id'].astype(int)
        df['t_id'] = df['t_id'].astype(int)
        df['ts'] = df['ts'].astype(int)
        df['tuples'] = pd.to_numeric(df['tuples'], errors='coerce').fillna(0).astype(int)

        # Add a window column
        df['window'] = df['t_id'] // window_size

        # Separate begin and end events
        begin_df = df[df['ph'] == 'B'].copy()
        end_df = df[df['ph'] == 'E'].copy()

        # Merge begin and end events on `p_id`, `t_id`, and `window`
        merged = pd.merge(
            begin_df, end_df, on=['run_id', 'p_id', 't_id', 'window'], suffixes=('_begin', '_end')
        )

        # Calculate latency and throughput
        merged['latency'] = merged['ts_end'] - merged['ts_begin'] #microseconds
        if (merged['latency'] <= 0).any():
            num_nul= (merged['latency'] <= 0).sum()
            print(f"Warning: Some tasks have {num_nul} non-positive latency and will be excluded from metrics.")
            merged = merged[merged['latency'] > 0].copy()
        merged['throughput'] = merged['tuples_begin'] / (merged['latency'] / 1e6)  # Convert to Tuples/second

        num_reps = merged['run_id'].nunique()

        # Group by run_id and produce pandas Series so we can use .mean() / np.mean() easily
        grouped = merged.groupby('run_id')

        # Total tasks time per run (in microseconds)
        total_tasks_time_by_run = grouped['latency'].sum()

        # Full query duration per run (in microseconds)
        full_query_duration_by_run = (grouped['ts_end'].max() - grouped['ts_begin'].min())

        # Aggregate means (already in seconds / counts)
        mean_full_query_duration = full_query_duration_by_run.mean()
        mean_total_tasks_time = total_tasks_time_by_run.mean()


        # Total skipped tasks across all begins minus matched (aggregate across all runs)
        total_skipped_tasks = len(begin_df) - len(merged)


        #TODO: total skipped tasks by p_id

        # Skipped tasks per run: begins per run minus matched tasks per run
        #starts_by_run = df[df['ph'] == 'B'].groupby('run_id').size()
        #matched_by_run = grouped.size()
        #total_skipped_tasks_by_run = starts_by_run.reindex(matched_by_run.index, fill_value=0) - matched_by_run


        #mean_total_skipped_tasks = total_skipped_tasks_by_run.mean()



        # Count starts per run/p_id/window (to compute skipped = starts - matched)
        starts = begin_df.groupby(['run_id', 'p_id', 'window']).size().rename('starts')

        # Per-run, per-pipeline-window aggregates from matched tasks
        grouped_run = merged.groupby(['run_id', 'p_id', 'window']).agg(
            count=('latency', 'size'),
            total_tuples=('tuples_begin', 'sum'),
            sum_latency=('latency', 'sum'),          # microseconds
            mean_latency=('latency', 'mean'),
            std_latency=('latency', 'std'),
            mean_tp=('throughput', 'mean'),
            std_tp=('throughput', 'std'),
            sum_tp=('throughput', 'sum')         # for completeness
        )

        # Join starts to compute skipped per run/p_id/window
        grouped_run = grouped_run.join(starts, how='left')
        grouped_run['starts'] = grouped_run['starts'].fillna(0).astype(int)
        grouped_run['skipped'] = grouped_run['starts'] - grouped_run['count']
        grouped_run['skipped'] = grouped_run['skipped'].clip(lower=0)

        # Reset index for per-window averaging across runs
        grouped_run = grouped_run.reset_index()

        # Average windowed metrics across runs (mean and std across runs)
        windowed_stats = grouped_run.groupby(['p_id', 'window']).agg(
            runs=('run_id', 'nunique'),
            count_mean=('count', 'mean'),
            count_std=('count', 'std'),
            total_tuples_mean=('total_tuples', 'mean'),
            total_tuples_std=('total_tuples', 'std'),
            sum_latency_mean=('sum_latency', 'mean'),      # microseconds
            sum_latency_std=('sum_latency', 'std'),
            mean_latency_mean=('mean_latency', 'mean'), # per-task average latency (microseconds)
            mean_latency_std=('mean_latency', 'std'),
            mean_tp_mean=('mean_tp', 'mean'),
            mean_tp_std=('mean_tp', 'std'),
            skipped_mean=('skipped', 'mean'),
            skipped_std=('skipped', 'std')
        ).reset_index().fillna(0)

        # Build per-run, per-pipeline totals (to be averaged across runs for pipeline-level stats)
        per_run_pipeline = merged.groupby(['run_id', 'p_id']).agg(
            count=('latency', 'size'),
            sum_tuples=('tuples_begin', 'sum'),
            sum_latency=('latency', 'sum'),                 # microseconds (compute time)
            mean_latency=('latency', 'mean'),
            sum_tp=('tp_task', 'sum')
        )

        # compute pipeline wall-time per run (wall time = max end - min begin per run/p_id)
        run_wall = merged.groupby(['run_id', 'p_id']).agg(
            wall_start=('ts_begin', 'min'),
            wall_end=('ts_end', 'max')
        )
        run_wall['wall_time'] = run_wall['wall_end'] - run_wall['wall_start']  # microseconds
        per_run_pipeline = per_run_pipeline.join(run_wall['wall_time'], how='left')
        per_run_pipeline = per_run_pipeline.reset_index()

        # skipped per run/pipeline (sum of skipped windows)
        skipped_per_run_pipeline = per_run_window.groupby(['run_id', 'p_id'])['skipped'].sum().rename('skipped')
        per_run_pipeline = per_run_pipeline.join(skipped_per_run_pipeline, on=['run_id', 'p_id'], how='left')
        per_run_pipeline['skipped'] = per_run_pipeline['skipped'].fillna(0).astype(int)

        # compute per-run derived tps to allow mean-of-runs later
        per_run_pipeline['comp_tp_run'] = per_run_pipeline.apply(
            lambda r: (r['sum_tuples'] / (r['sum_latency'] / 1e6)) if r['sum_latency'] > 0 else 0, axis=1
        )  # tuples/sec using compute time
        per_run_pipeline['eff_tp_run'] = per_run_pipeline.apply(
            lambda r: (r['sum_tuples'] / (r['wall_time'] / 1e6)) if r['wall_time'] > 0 else 0, axis=1
        )

        # Now aggregate across runs for each pipeline (compact: means/stds of core sums & times)
        pipeline_stats = per_run_pipeline.groupby('p_id').agg(
            runs=('run_id', 'nunique'),
            count_mean=('count', 'mean'),
            count_std=('count', 'std'),
            sum_tuples_mean=('sum_tuples', 'mean'),
            sum_tuples_std=('sum_tuples', 'std'),
            sum_latency_mean=('sum_latency', 'mean'),     # microseconds
            sum_latency_std=('sum_latency', 'std'),
            wall_time_mean=('wall_time', 'mean'),          # microseconds
            wall_time_std=('wall_time', 'std'),
            mean_tp_mean=('sum_tp', 'mean'),               # average of per-run sum_tp (optional)
            skipped_mean=('skipped', 'mean'),
            skipped_std=('skipped', 'std'),
            comp_tp_mean_of_runs=('comp_tp_run', 'mean'),
            comp_tp_std_of_runs=('comp_tp_run', 'std'),
            eff_tp_mean_of_runs=('eff_tp_run', 'mean'),
            eff_tp_std_of_runs=('eff_tp_run', 'std')
        ).reset_index().fillna(0)

        # Provide derived comp/eff tp based on aggregated means (useful for some comparisons).
        # These are computed as mean_sum_tuples / mean_sum_latency  (and / mean_wall_time).
        def safe_div(a, b):
            return (a / b) if b and b > 0 else 0  # b in microseconds #-> convert to seconds

        pipeline_stats['comp_tp_from_means'] = pipeline_stats.apply(
            lambda r: safe_div(r['sum_tuples_mean'], r['sum_latency_mean']), axis=1
        )
        pipeline_stats['eff_tp_from_means'] = pipeline_stats.apply(
            lambda r: safe_div(r['sum_tuples_mean'], r['wall_time_mean']), axis=1
        )


        global_stats = {
            'total_tasks_time': mean_total_tasks_time,
            'full_query_duration': mean_full_query_duration ,
            'total_skipped_tasks': total_skipped_tasks
        }

        run_stats = {
            'total_tasks_time_by_run': total_tasks_time_by_run ,
            'full_query_duration_by_run': full_query_duration_by_run
        }

        return windowed_stats, pipeline_stats, global_stats, run_stats, metadata

    except Exception as e:
        print(f"Error processing CSV with pandas: {e}")
        return None, None










def write_results_with_pandas(base_directory, windowed_results, pipeline_results):
    """Write results to CSV files."""
    # Write windowed results
    windowed_path = os.path.join(base_directory, 'results_windowed.csv')
    windowed_results.to_csv(windowed_path, index=False)
    print(f"Windowed results written to {windowed_path}")

    # Write pipeline results
    pipeline_path = os.path.join(base_directory, 'results_pipeline.csv')
    pipeline_results.to_csv(pipeline_path, index=False)
    print(f"Pipeline results written to {pipeline_path}")

def extract_metadata_from_filename(file_path):
    """Extract metadata (layout, buffer size, threads, query) from filename."""
    # Get just the filename from path
    filename = os.path.basename(file_path)

    # Fix duplicate filenames like "file.jsonfile.json"
    if filename.endswith(".csv"):
        duplicate_pos = filename.find(".csv", 0, -5)
        if duplicate_pos > 0:
            filename = filename[:duplicate_pos + 5]

    metadata = {}

    query_id= re.search(r'_query(\d+).json', filename)
    if query_id:
        metadata['query_id'] = query_id.group(1)

    # Extract layout with more specific pattern
    if '_ROW_LAYOUT_' in filename:
        metadata['layout'] = 'ROW_LAYOUT'
    elif '_COLUMNAR_LAYOUT_' in filename:
        metadata['layout'] = 'COLUMNAR_LAYOUT'

    # Extract buffer size with more specific pattern
    buffer_match = re.search(r'_buffer(\d+)_', filename)
    if buffer_match:
        metadata['buffer_size'] = buffer_match.group(1)

    # Extract thread count with more specific pattern
    threads_match = re.search(r'_threads(\d+)_', filename)
    if threads_match:
        metadata['threads'] = threads_match.group(1)

    # Extract query number with more specific pattern
    query_match = re.search(r'_query(\d+)', filename)
    if query_match:
        metadata['query'] = query_match.group(1)

    return metadata

def main():

    # Get directory and optional trace files from command line
    parser = argparse.ArgumentParser(description='Process trace files')
    parser.add_argument('benchmark_directory', help='Directory to save results')
    parser.add_argument('--trace-files', nargs='+', help='List of trace files to process')
    parser.add_argument('--num-reps', type=int, default=2, help='Number of repetitions for averaging')
    args = parser.parse_args()

    base_directory = args.benchmark_directory

    # Find all trace files in the logs directory if not provided directly
    trace_files = args.trace_files or []

    if not trace_files:
        print("No trace files found to process")
        return

    print(f"Processing {len(trace_files)} trace files in parallel...")

    # -trace to csv#
    # -get all csvs for same configuration (run 1, 2 etc.)
    # calculate metrics (windowed and average) + needed for plots, total_time etc.
    # -average over repetitions
    # delete old data files
    # combine average results to main result file


    #find first underscore in filename and group by suffix

    config_groups = defaultdict(list)
    for trace_file in trace_files:
        filename = os.path.basename(trace_file)
        first_underscore = filename.find('_')
        config_key = filename[first_underscore:]
        config_groups[config_key].append(trace_file)

    query_config = get_config_from_file(Path(base_directory) / 'query_configs.csv')

    # Process all trace files in parallel
    results_iter = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = [executor.submit(process_csv, paths) for paths in config_groups.values()]
        for fut in concurrent.futures.as_completed(futures):
            try:
                results_iter.append(fut.result())
            except Exception as e:
                print(f"Error in worker: {e}")


    # Process data for CSV output
    csv_data = []
    csv_rows = []
    all_fields = set(['windowed_stats', 'pipeline_stats', 'global_stats', 'run_stats', 'metadata'])



    for result in results_iter:
        if not result or len(result) < 5:
            continue
        windowed_stats, pipeline_stats, global_stats, run_stats, metadata = result

        # metadata safety
        query_id = metadata.get('query_id') if metadata else None
        config = query_config.get(query_id, {}) if query_id else {}

        # Skip multi-operator chains if desired (keeps original behavior)
        operator_chain = config.get('operator_chain', ['unknown'])
        if len(operator_chain) > 1:
            # still write per-config files but skip adding to consolidated CSV to mirror prior behavior
            pass

        # Prepare buffer dir and write per-config files if DataFrames present
        try:
            buffer_dir = Path(base_directory) / operator_chain[0] / f"bufferSize{metadata.get('buffer_size','')}"
            buffer_dir.mkdir(parents=True, exist_ok=True)
            if windowed_stats is not None:
                windowed_stats.to_csv(buffer_dir / "results_windowed.csv", index=False)
            if pipeline_stats is not None:
                pipeline_stats.to_csv(buffer_dir / "results_pipeline.csv", index=False)
        except Exception as e:
            print(f"Warning: error writing per-config files: {e}")

        # Base row with global metrics
        row = {
            'layout': metadata.get('layout', ''),
            'buffer_size': metadata.get('buffer_size', ''),
            'threads': metadata.get('threads', ''),
            'query': metadata.get('query', ''),
            'total_tasks_time': global_stats.get('total_tasks_time', 0) if global_stats else 0,
            'full_query_duration': global_stats.get('full_query_duration', 0) if global_stats else 0,
            'total_skipped_tasks': global_stats.get('total_skipped_tasks', 0) if global_stats else 0
        }

        # Pull pipeline-level metrics from pipeline_stats DataFrame if present
        if pipeline_stats is not None and not pipeline_stats.empty:
            # ensure p_id column exists
            if 'p_id' in pipeline_stats.columns:
                for _, prow in pipeline_stats.iterrows():
                    pid = prow['p_id']
                    # core aggregated sums/means (matching compute_stats scope)
                    sum_tuples_mean = prow.get('sum_tuples_mean', prow.get('sum_tuples', 0))
                    sum_latency_mean = prow.get('sum_latency_mean', prow.get('sum_latency', 0))  # microseconds
                    count_mean = prow.get('count_mean', prow.get('count', 0))
                    wall_time_mean = prow.get('wall_time_mean', prow.get('wall_time', 0))

                    # mean latency per task analogous to compute_stats: sum_time / count
                    mean_latency = (sum_latency_mean / count_mean) if count_mean and count_mean > 0 else prow.get('mean_latency_mean', 0)

                    # comp/eff throughput derived from means (convert microseconds to seconds)
                    comp_tp_from_means = (sum_tuples_mean / (sum_latency_mean / 1e6)) if sum_latency_mean and sum_latency_mean > 0 else 0
                    eff_tp_from_means = (sum_tuples_mean / (wall_time_mean / 1e6)) if wall_time_mean and wall_time_mean > 0 else 0

                    # per-run mean-of-runs if present prefer those
                    comp_tp_mean_of_runs = prow.get('comp_tp_mean_of_runs', None)
                    eff_tp_mean_of_runs = prow.get('eff_tp_mean_of_runs', None)

                    # skipped statistics
                    skipped_mean = prow.get('skipped_mean', 0)
                    skipped_std = prow.get('skipped_std', 0)
                    runs = prow.get('runs', 0)

                    # Add pipeline-specific fields (no truncated fields like time_pct)
                    base_keys = {
                        f'pipeline_{pid}_runs': runs,
                        f'pipeline_{pid}_sum_tuples_mean': sum_tuples_mean,
                        f'pipeline_{pid}_sum_latency_mean': sum_latency_mean,
                        f'pipeline_{pid}_wall_time_mean': wall_time_mean,
                        f'pipeline_{pid}_count_mean': count_mean,
                        f'pipeline_{pid}_mean_latency': mean_latency,
                        f'pipeline_{pid}_comp_tp_from_means': comp_tp_from_means,
                        f'pipeline_{pid}_eff_tp_from_means': eff_tp_from_means,
                        f'pipeline_{pid}_comp_tp_mean_of_runs': comp_tp_mean_of_runs if comp_tp_mean_of_runs is not None else comp_tp_from_means,
                        f'pipeline_{pid}_eff_tp_mean_of_runs': eff_tp_mean_of_runs if eff_tp_mean_of_runs is not None else eff_tp_from_means,
                        f'pipeline_{pid}_skipped_mean': skipped_mean,
                        f'pipeline_{pid}_skipped_std': skipped_std
                    }

                    for k, v in base_keys.items():
                        row[k] = v if v is not None else 0
                        all_fields.add(k)
            else:
                # fallback: if pipeline_stats is a dict-like or empty, attempt best-effort extraction
                try:
                    pdata = pipeline_stats if isinstance(pipeline_stats, dict) else {}
                    for pid, p in pdata.items():
                        key_prefix = f'pipeline_{pid}_'
                        row[key_prefix + 'sum_tuples_mean'] = p.get('sum_tuples', 0)
                        row[key_prefix + 'sum_latency_mean'] = p.get('sum_time', 0)
                        row[key_prefix + 'comp_tp'] = p.get('comp_tp', 0)
                        row[key_prefix + 'eff_tp'] = p.get('eff_tp', 0)
                        all_fields.update([key_prefix + k for k in ['sum_tuples_mean', 'sum_latency_mean', 'comp_tp', 'eff_tp']])
                except Exception:
                    pass

        csv_rows.append(row)

    # Ensure we have at least some fields in consistent order and write consolidated CSV
    out_csv_path = os.path.join(base_directory, os.path.basename(base_directory) + ".csv")
    if csv_rows:
        all_fields = sorted(set().union(*(r.keys() for r in csv_rows)))
    with open(out_csv_path, 'w', newline='') as f:
        if csv_rows:
            fieldnames = sorted(all_fields)
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in csv_rows:
                # fill missing fields with 0
                for fn in fieldnames:
                    if fn not in r:
                        r[fn] = 0
                writer.writerow(r)
        else:
            # write basic header only
            writer = csv.writer(f)
            writer.writerow(sorted(list(all_fields)))

    print(f"Consolidated CSV written to {out_csv_path}")

if __name__ == "__main__":
    main()