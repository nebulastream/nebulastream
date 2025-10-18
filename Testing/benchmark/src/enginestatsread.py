#!/usr/bin/env python3
import sys
import os
import csv
import argparse
import numpy as np
import pandas as pd
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
        dfs = [pd.read_csv(f) for f in trace_paths]

        # concatenate over all number of runs
        df = pd.concat(dfs, keys=range(len(dfs)), names=['run_id'])

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
        merged['latency'] = merged['ts_end'] - merged['ts_begin'] #ms
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
            sum_tp=('throughput', 'sum'),
            mean_latency=('latency', 'mean'),
            std_latency=('latency', 'std'),
            mean_tp=('throughput', 'mean'),
            std_tp=('throughput', 'std')
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
            sum_tp_mean=('sum_tp', 'mean'),
            sum_tp_std=('sum_tp', 'std'),
            mean_latency_mean=('mean_latency', 'mean'),
            mean_latency_std=('mean_latency', 'std'),
            mean_tp_mean=('mean_tp', 'mean'),
            mean_tp_std=('mean_tp', 'std'),
            skipped_mean=('skipped', 'mean'),
            skipped_std=('skipped', 'std')
        ).reset_index().fillna(0)

        # Pipeline-level aggregation: average the windowed metrics across windows (as requested)
        pipeline_stats = windowed_stats.groupby('p_id').agg(
            windows=('window', 'nunique'),
            total_tuples_mean=('total_tuples_mean', 'mean'),
            total_tuples_std=('total_tuples_std', 'mean'),
            sum_tp_mean=('sum_tp_mean', 'mean'),
            sum_tp_std=('sum_tp_std', 'mean'),
            mean_latency_mean=('mean_latency_mean', 'mean'),
            mean_latency_std=('mean_latency_std', 'mean'),
            mean_tp_mean=('mean_tp_mean', 'mean'),
            mean_tp_std=('mean_tp_std', 'mean'),
            skipped_mean=('skipped_mean', 'mean'),
            skipped_std=('skipped_std', 'mean')
        ).reset_index().fillna(0)


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
    parser.add_argument('base_directory', help='Directory to save results')
    parser.add_argument('--trace-files', nargs='+', help='List of trace files to process')
    parser.add_argument('--num-reps', type=int, default=2, help='Number of repetitions for averaging')
    args = parser.parse_args()

    base_directory = args.base_directory

    # Find all trace files in the logs directory if not provided directly
    trace_files = args.trace_files or []
    if not trace_files:
        logs_directory = os.path.join(base_directory, "logs")
        if os.path.exists(logs_directory):
            for file in os.listdir(logs_directory):
                if file.endswith('.csv') and 'GoogleEventTrace' in file:
                    trace_files.append(os.path.join(logs_directory, file))

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

    # Process all trace files in parallel
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        results = executor.map(process_csv, config_groups)


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
            for metric in ['comp_tp', 'eff_tp', 'total_tuples', 'time_pct', 'mean_latency', 'skipped_tasks']:
                field_name = f'pipeline_{pid}_{metric}'
                row[field_name] = p.get(metric, 0)
                all_fields.add(field_name)

        csv_data.append(row)

    # Write the results to a text file
    with open(os.path.join(base_directory, 'results.txt'), 'w') as f:
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

    # Write CSV with all possible fields to the base directory
    csv_path = os.path.join(base_directory, os.path.basename(base_directory) + ".csv")
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

    print(f"Results written to {os.path.join(base_directory, 'results.txt')}")
    print(f"CSV written to {csv_path}")

if __name__ == "__main__":
    main()