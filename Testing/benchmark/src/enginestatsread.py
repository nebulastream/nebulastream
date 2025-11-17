#!/usr/bin/env python3
import sys
import os
import csv
import argparse
import numpy as np
import pandas as pd
from collections import defaultdict
from utils import get_config_from_file, extract_metadata_from_filename
from pathlib import Path
import concurrent
import multiprocessing
import concurrent.futures
import json
import re
import gc

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


def process_csv(trace_paths):
    """ Process a single CSV/JSON trace(s) and return aggregated results.

    window_size is in seconds; internal timestamps are in microseconds so we
    compute windows as ts // (window_size * 1_000_000).
    """
    try:
        dfs = []
        run = 0
        full_query_durations = {}
        pipeline_times_by_run = {}
        empty= 0
        print(f"Processing {len(trace_paths)} trace files...")
        if len(trace_paths) == 0:
            print("No trace files provided.")
            return None, None, None, None, {}
        print("e.g.:" + trace_paths[0])
        for trace_file in trace_paths:
            if os.path.getsize(trace_file) == 0:
                continue
            with open(trace_file, 'r') as f:
                trace_data = json.load(f)

            # Extract trace events
            events  = trace_data.get('traceEvents', [])

            rows = []
            total_time = {}
            pipeline_times = {}
            for event in events:
                if event.get('ph') in ['B', 'E'] and event.get('cat') == 'task' and 'args' in event:
                    args = event['args']
                    # Ignore the first 9 digits of the timestamp
                    p_id = int(args.get('pipeline_id', -1))
                    t_id = int(args.get('task_id', -1))
                    ph = event.get('ph', '')
                    #reduced_ts = (event['ts'] % 1_000_000_000)
                    ts = event['ts'] #% 1_000_000_000
                    latency = 0
                    tuples = 0
                    if ph == 'E':
                        tuples = 0
                        latency = event.get('dur', -1)
                    else:
                        latency = 0
                        tuples = args.get('tuples', 0)

                    rows.append({
                        'p_id': p_id,
                        't_id': t_id,
                        'ph': ph,
                        'ts': ts,
                        'tuples': tuples,
                        'latency': latency
                    })
                elif event.get('cat') == 'query':
                    if event.get('ph') == 'B':
                        total_time['begin'] = event.get('ts', 0)
                    elif event.get('ph') == 'E':
                        total_time['end'] = event.get('ts', 0)
                elif event.get('cat') == 'pipeline' and 'args' in event:
                    args = event['args']
                    p_id = int(args.get('pipeline_id', -1))
                    pipe_time = pipeline_times.get(p_id, {})
                    if event.get('ph') == 'B':
                        pipe_time['begin'] = event.get('ts', 0)
                    elif event.get('ph') == 'E':
                        pipe_time['end'] = event.get('ts', 0)
                    pipeline_times[p_id] = pipe_time

            if not rows:
                continue
            df = pd.DataFrame(rows)

            if 'begin' in total_time and 'end' in total_time:
                full_query_durations[run] = (total_time.get('end', 0) - total_time.get('begin', 0))
            elif 'begin' in total_time:
                #print("Warning: Missing query end event in trace. Using max_ts")
                max_ts = df['ts'].max()
                full_query_durations[run] = (max_ts - total_time.get('begin', 0))
            else:
                print("Warning: Missing query begin event in trace: ", full_query_durations)
                print("From file: " + trace_file)

            times = {}
            for pipe in pipeline_times:
                ptime = pipeline_times[pipe]
                if 'begin' in ptime and 'end' in ptime:
                    duration = ptime.get('end', 0) - ptime.get('begin', 0)

                    times[pipe] = duration
                elif 'begin' in ptime:
                    #print(f"Warning: Missing pipeline {pipe} end event in trace. Using max_ts")
                    df_pipe = df[df['p_id'] == pipe]
                    max_ts = df_pipe['ts'].max()
                    duration = max_ts - ptime.get('begin', 0)
                    times[pipe] = duration
                else:

                    print(f"Warning: Missing pipeline {pipe} begin or end event in trace.")
            pipeline_times_by_run[run] = times
            run += 1

            # Normalize timestamps per-file to start at zero (microseconds).
            # Avoid modulo wrap that can reorder events and cause negative latencies.
            min_timestamp = df['ts'].min()# % 1_000_000_000
            df['ts'] = (df['ts']  -  min_timestamp)

            dfs.append(df)
        if empty > 0:
            print(f"Warning: {empty} empty trace files skipped.")
            if empty == len(trace_paths):
                print("All trace files are empty.")
                return None, None, None, None, {}

        if not dfs:
            print("No valid task events found in provided trace files.")
            return None, None, None, None, {}

        # concatenate across runs; key -> run_id
        df = pd.concat(dfs, keys=range(len(dfs)), names=['run_id']).reset_index(level=0).reset_index(drop=True)
        metadata = extract_metadata_from_filename(trace_paths[0])

        # ensure types
        df['run_id'] = df['run_id'].astype(int)
        df['t_id'] = df['t_id'].astype(int)
        df['ts'] = df['ts'].astype(int)
        df['tuples'] = pd.to_numeric(df['tuples'], errors='coerce').fillna(0).astype(int)
        df['latency'] = pd.to_numeric(df['latency'], errors='coerce').fillna(0).astype(int)


        max_ts = df['ts'].max()

        window_size = max_ts // 10000 # at max 10k col per file
        if window_size < 1:
            window_size = 1
        num_windows = max_ts // window_size #<= 10000


        # Separate begin and end events
        begin_df = df[df['ph'] == 'B'].copy()
        end_df = df[df['ph'] == 'E'].copy()

        # Merge begin and end on run_id, p_id, t_id, window
        merged = pd.merge(
            begin_df, end_df, on=['run_id', 'p_id', 't_id'], suffixes=('_begin', '_end')
        )


        #combine begin and end events taking tuples from begin and latency from end
        merged['latency'] = merged['latency_end'] #microseconds
        merged['tuples'] = merged['tuples_begin']


        max_size = np.max([begin_df['tuples'].size, end_df['tuples'].size])

        diff = int(np.abs(max_size - merged['tuples_begin'].size))

        if diff > 0 and diff > max_size / 10000:
            print("Warning: Total skipped tasks:")
            print(f"{diff} out of")
            print(f"{int(max_size)} tasks across all runs.")
            if max_size < 550:
                print(trace_file)
            #print(f"{int(np.abs(merged['tuples_begin'].size - merged['tuples_end'].size))}")
            #print(f"Total tasks time across all runs (s): {total_tasks_time_by_run.sum() / 1e6}")



        #remove emtpy latency -1 if tuples are 0
        mask_invalid = (merged['latency'] == -1) & (merged['tuples'] == 0)
        if mask_invalid.any():
            #print(f"Warning: {mask_invalid.sum()} tasks with no duration.")
            merged.loc[mask_invalid, 'latency'] = 0


        #discard ts and store max_ts by run
        merged['max_ts'] = merged.groupby('run_id')['ts_end'].transform('max')
        merged['duration'] = merged['ts_end'] - merged['ts_begin']

        # Compute window number based on midpoint
        merged['window'] = ((merged['ts_begin'] + merged['ts_end']) // 2 // window_size).astype(int)

        abs_diff = (merged['latency'] - merged['duration']).abs()
        mask_inconsistent = abs_diff > 1
        diff = int(mask_inconsistent.sum())
        if diff > 0:
            print(f"Warning: {diff} tasks have inconsistent latency values between duration and timestamps.")
        else:
            merged['latency'] = merged['duration']
            #merged = merged.drop(columns=['ts_begin', 'ts_end', 'duration'])



        if (merged['latency'] <= 0).any():
            #remove 0 latencies 0 tuples tasks
            merged = merged[~(merged['latency'] == 0)].copy()

            num_nul= (merged['latency'] < 0).sum()
            tuples = (merged[merged['latency'] < 0]['tuples']).sum()
            if num_nul > 0 and tuples > 0:
                print(f"Warning: Some tasks have {num_nul} non-positive latency and will be excluding {tuples} tuples from metrics.")
                #print(f"{(merged['latency'] ==  -1).sum()} tasks have empty latency.")
                #print(f"{(merged['latency_begin'] ==  0).sum()} begin tasks have empty latency.")
                merged = merged[merged['latency'] > 0 and merged['latency'] >= 0].copy()


        # throughput per task (tuples/sec)
        merged['throughput'] = merged['tuples'] / (merged['latency'] / 1e6)

        empty_tuples = (merged['tuples'] <= 0).sum()
        empty_tp = (merged['throughput'] <= 0).sum()
        if empty_tuples > 0 or empty_tp > 0:
            print(f"empty tuples {empty_tuples} / {merged['tuples'].size}, zero tp {empty_tp} / {merged['throughput'].size}")
            print(trace_file)
            latency_avg = merged['latency'].mean() / 1e6
            latency_min = merged['latency'].min() / 1e6
            latency_max = merged['latency'].max() / 1e6
            avg = merged['throughput'].mean()
            min = merged['throughput'].min()
            max = merged['throughput'].max()
            print(f"Throughput stats (tuples/sec): avg={avg}, min={min}, max={max}")
            print(f"Latency stats (sec): avg={latency_avg}, min={latency_min}, max={latency_max}")
        # Calculate skipped tasks per (run_id, p_id, window):
        # skipped = max(begin_count, end_count) - matched_count
        # derive window for begin/end using ts // window_size (same coarse bins)
        b = begin_df.copy()
        b['window'] = (b['ts'] // window_size).astype(int)
        begin_grp = b.groupby(['run_id', 'p_id', 'window']).size().rename('begin_count')

        e = end_df.copy()
        e['window'] = (e['ts'] // window_size).astype(int)
        end_grp = e.groupby(['run_id', 'p_id', 'window']).size().rename('end_count')

        matched_grp = merged.groupby(['run_id', 'p_id', 'window']).size().rename('matched_count')

        counts = pd.concat([begin_grp, end_grp, matched_grp], axis=1).fillna(0).reset_index()
        counts['skipped'] = (counts[['begin_count', 'end_count']].max(axis=1) - counts['matched_count']).clip(lower=0).astype(int)

        # Join skipped back onto merged rows
        merged = merged.merge(counts[['run_id', 'p_id', 'window', 'skipped']], on=['run_id', 'p_id', 'window'], how='left')
        merged['skipped'] = merged['skipped'].fillna(0).astype(int)

        #print(f"Total skipped tasks {counts['skipped'].sum()} vs diff {diff}")



        total_tasks_time_by_pipeline = {}

        for run_id in range(run):
            times = {}
            for p_id in merged['p_id'].unique():
                mask = (merged['run_id'] == run_id) & (merged['p_id'] == p_id)
                times[p_id] = merged.loc[mask, 'latency'].sum()
                #merged['skipped'] = np.abs(merged.loc[mask, 'tuples_begin'].size - merged.loc[mask, 'tuples_end'].size)
            total_tasks_time_by_pipeline[run_id] = times

        # Per-run, per-pipeline-window aggregates (from matched tasks)
        grouped_run = merged.groupby(['run_id', 'p_id', 'window']).agg(
            count=('t_id', 'size'),
            total_tuples=('tuples', 'sum'),
            total_skipped=('skipped', 'sum'),
            sum_latency=('latency', 'sum'),          # microseconds
            mean_latency=('latency', 'mean'),
            std_latency=('latency', 'std'),
            mean_tp=('throughput', 'mean'),
            std_tp=('throughput', 'std'),
            sum_tp=('throughput', 'sum'),
            max_ts=('max_ts', 'max')
        )

        # Reset index for per-window averaging across runs and keep a copy
        grouped_run = grouped_run.reset_index()
        per_run_window = grouped_run.copy()

        # Average windowed metrics across runs (mean and std across runs)
        windowed_stats = per_run_window.groupby(['p_id', 'window']).agg(
            runs=('run_id', 'nunique'),
            count_mean=('count', 'mean'),
            count_std=('count', 'std'),
            total_skipped_mean=('total_skipped', 'mean'),
            total_skipped_std=('total_skipped', 'std'),
            total_tuples_mean=('total_tuples', 'mean'),
            total_tuples_std=('total_tuples', 'std'),
            sum_latency_mean=('sum_latency', 'mean'),      # microseconds
            sum_latency_std=('sum_latency', 'std'),
            mean_latency_mean=('mean_latency', 'mean'), # per-task average latency (microseconds)
            mean_latency_std=('mean_latency', 'std'),
            mean_tp_mean=('mean_tp', 'mean'),
            mean_tp_std=('mean_tp', 'std'),
            sum_tp_mean=('sum_tp', 'mean'),
        ).reset_index().fillna(0)

        # Build per-run, per-pipeline totals (to be averaged across runs for pipeline-level stats)
        per_run_pipeline = merged.groupby(['run_id', 'p_id']).agg(
            count=('t_id', 'size'),
            sum_tuples=('tuples', 'sum'),
            sum_latency=('latency', 'sum'),                 # microseconds (compute time)
            mean_latency=('latency', 'mean'),
            std_latency=('latency', 'std'),
            sum_tp=('throughput', 'sum'),
            total_skipped=('skipped', 'sum'),
            max_ts=('max_ts', 'max')
        )
        # Full query duration per run (microseconds)
        #full_query_duration_by_run = full_query_durations
        #(grouped['ts_end'].max() - grouped['ts_begin'].min())
        full_query_duration_rows = []
        for run_id, time in full_query_durations.items():
            full_query_duration_rows.append({'run_id': run_id, 'full_query_duration': time})
        full_query_duration_by_run = pd.DataFrame(full_query_duration_rows, columns=['run_id', 'full_query_duration'])

        p_time_rows = []
        for run_id, pipes in pipeline_times_by_run.items():
            for p_id, time in pipes.items():
                p_time_rows.append({'run_id': run_id, 'p_id': p_id, 'wall_time': time})
        p_time = pd.DataFrame(p_time_rows, columns=['run_id', 'p_id', 'wall_time'])

        total_tasks_rows = []
        for run_id, pipes in total_tasks_time_by_pipeline.items():
            for p_id, time in pipes.items():
                total_tasks_rows.append({'run_id': run_id, 'p_id': p_id, 'total_tasks_time': time})
        total_tasks_time_by_pipe = pd.DataFrame(total_tasks_rows, columns=['run_id', 'p_id', 'total_tasks_time'])

        # Aggregate means (already in seconds / counts)
        mean_full_query_duration = full_query_duration_by_run['full_query_duration'].mean()
        std_full_query_duration = full_query_duration_by_run['full_query_duration'].std()
        per_run_total_tasks = total_tasks_time_by_pipe.groupby('run_id')['total_tasks_time'].sum()
        total_tasks_time_mean = per_run_total_tasks.mean()
        total_tasks_time_std = per_run_total_tasks.std()

        #per_run_pipeline = pd.merge(per_run_pipeline, full_query_duration_by_run, on=['run_id', 'p_id'])
        per_run_pipeline = pd.merge(per_run_pipeline.reset_index(), p_time, on=['run_id', 'p_id'], how='left')
        per_run_pipeline = pd.merge(per_run_pipeline, total_tasks_time_by_pipe, on=['run_id', 'p_id'], how='left')

        per_run_pipeline['comp_tp_run'] = per_run_pipeline.apply(
            lambda r: (r['sum_tuples'] / (r['sum_latency'] / 1e6)) if r['sum_latency'] > 0 else 0, axis=1
        )
        per_run_pipeline['eff_tp_run'] = per_run_pipeline.apply(
            lambda r: (r['sum_tuples'] / (r['wall_time'] / 1e6)) if r['wall_time'] > 0 else 0, axis=1
        )

        latency_null = (per_run_pipeline['mean_latency'] <= 0).sum()
        if latency_null > 0:
            print(f"Warning: Some runs have zero mean latency: {latency_null} / {per_run_pipeline.shape[0]}")

        wall_time_null = (per_run_pipeline['wall_time'] <= 0).sum()
        if wall_time_null > 0:
            print(f"Warning: Some runs have zero wall time: {wall_time_null} / {per_run_pipeline.shape[0]}")

        sum_tuples_null = (per_run_pipeline['sum_tuples'] <= 0).sum()
        if sum_tuples_null > 0:
            print(f"Warning: Some runs have zero sum_tuples: {sum_tuples_null} / {per_run_pipeline.shape[0]}")

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
            skipped_mean=('total_skipped', 'mean'),
            comp_tp_mean_of_runs=('comp_tp_run', 'mean'),
            comp_tp_std_of_runs=('comp_tp_run', 'std'),
            eff_tp_mean_of_runs=('eff_tp_run', 'mean'),
            eff_tp_std_of_runs=('eff_tp_run', 'std'),
            max_ts=('max_ts', 'max'),
            max_ts_std=('max_ts', 'std'),
            max_ts_mean=('max_ts', 'mean'),
        ).reset_index().fillna(0)

        # Provide derived comp/eff tp based on aggregated means
        #def safe_div(a, b):
        #    return (a / b) if b and b > 0 else 0  # b in microseconds #-> convert to seconds

        pipeline_stats['comp_tp_from_means'] = pipeline_stats.apply(
            lambda r: (r['sum_tuples_mean'] / (r['sum_latency_mean'] / 1e6)), axis=1
        )
        pipeline_stats['eff_tp_from_means'] = pipeline_stats.apply(
            lambda r: (r['sum_tuples_mean'] / (r['wall_time_mean'] / 1e6)), axis=1
        )
        #if pipeline_stats is not None:
        #    print("pipeline_stats dtypes:\n", pipeline_stats.dtypes)
        #    cols_check = [c for c in ['sum_tuples_mean','sum_latency_mean','wall_time_mean','mean_tp_mean','sum_tp_mean'] if c in pipeline_stats.columns]
        #    print("pipeline_stats sample:\n", pipeline_stats[cols_check].head())
        #    print("pipeline_stats stats:\n", pipeline_stats[cols_check].agg(['min','max','mean','count']))


        tuples_null = (pipeline_stats['sum_tuples_mean'] <= 0).sum()
        if tuples_null > 0:
            tuples_max = pipeline_stats['sum_tuples_mean'].max()
            tuples_min = pipeline_stats['sum_tuples_mean'].min()
            tuples_mean = pipeline_stats['sum_tuples_mean'].mean()
            print(f"Warning: Some pipelines have zero tuples processed: {tuples_null} / {pipeline_stats.shape[0]}")
            print(f"Tuples stats: max={tuples_max}, min={tuples_min}, mean={tuples_mean}")

        comp_tp_null = (pipeline_stats['comp_tp_from_means'] <= 0).sum()
        eff_tp_null = (pipeline_stats['eff_tp_from_means'] <= 0).sum()
        mean_tp_null = (pipeline_stats['mean_tp_mean'] <= 0).sum()
        if comp_tp_null > 0 or eff_tp_null > 0 or mean_tp_null > 0:
            print(f"Warning: Some pipelines have zero throughput computed from means: comp_tp={comp_tp_null}, eff_tp={eff_tp_null}, mean_tp={mean_tp_null} / {pipeline_stats.shape[0]}")


        global_stats = {
            'total_tasks_time_mean': total_tasks_time_mean,
            'total_tasks_time_std': total_tasks_time_std,
            'full_query_duration_mean': mean_full_query_duration,
            'full_query_duration_std': std_full_query_duration,
            #'total_skipped_tasks': total_skipped_tasks
        }

        try:
        # delete common large locals that hold DataFrames / lists
            del dfs
            del df
            del merged
            del begin_df
            del end_df
            del per_run_window
            del grouped_run
            del per_run_pipeline
            del trace_data
        except Exception:
            pass
        gc.collect()  # force collection in worker process

        return windowed_stats, pipeline_stats, global_stats, metadata

    except Exception as e:
        print(f"Error processing CSV with pandas: {e}")
        return None, None, None, {}

def results_to_global_csv(base_directory, results_iter, config):

    csv_rows = []
    for result in results_iter:
        if not result or len(result) != 4:
            print(f"Warning: Incomplete result len {len(result)}, skipping")
            continue
        windowed_stats, pipeline_stats, global_stats, metadata = result

        # Prepare buffer dir and write per-config files if DataFrames present
        try:
            #buffer_dir = Path(base_directory) / operator_chain[0] / f"bufferSize{metadata.get('buffer_size','')}"
            #buffer_dir.mkdir(parents=True, exist_ok=True)
            #result dir = query dir = parent /  parent / filename
            query_dir = Path(config['test_file']).parent
            #name= Path(metadata['filename']).stem.with_suffix('.csv')
            if metadata == {}:
                print(f"Warning: empty metadata for {config['test_file']}")
                continue
            layout = metadata['layout']
            threads = metadata.get('threads', '4')
            if windowed_stats is not None and not windowed_stats.empty:
                windowed_stats.to_csv(query_dir / f"threads{threads}" / f"results_windowed_{layout}.csv",mode='a', index=False)
                print(f"writing csv file to {query_dir / f"threads{threads}" / f"results_windowed_{layout}.csv"}")
            else:
                print(f"Warning: empty windowed_stats for {metadata['filename']}")
            if pipeline_stats is not None and not pipeline_stats.empty:
                pipeline_stats.to_csv(query_dir / f"threads{threads}" / f"results_pipelined_{layout}.csv", mode='a', index=False)
            else:
                print(f"Warning: empty pipeline_stats for {metadata['filename']}", flush=True)
        except Exception as e:
            print(f"Warning: error writing per-config files: {e}", flush=True)

        # Base row with global metrics
        row = {
            'layout': metadata.get('layout', ''),
            #'buffer_size': metadata.get('buffer_size', ''),
            'threads': metadata.get('threads', '4'),
            'query_id': metadata.get('query', ''),
            'total_tasks_time_mean': global_stats.get('total_tasks_time_mean', 0) if global_stats else 0,
            'total_tasks_time_std': global_stats.get('total_tasks_time_std', 0) if global_stats else 0,
            'full_query_duration_mean': global_stats.get('full_query_duration_mean', 0) if global_stats else 0,
            'full_query_duration_std': global_stats.get('full_query_duration_std', 0) if global_stats else 0,
            #'total_skipped_tasks': global_stats.get('total_skipped_tasks', 0) if global_stats else 0
        }

        config.update(row)

        row = config.copy()

        row.pop('test_file', None)  # remove test_file from final CSV
        row.pop('data_name', None)  # remove data_name from final CSV

        # Pull pipeline-level metrics from pipeline_stats DataFrame if present
        if pipeline_stats is not None and not pipeline_stats.empty:
            # ensure p_id column exists
            if 'p_id' in pipeline_stats.columns:
                for _, prow in pipeline_stats.iterrows():
                    pid = int(prow['p_id'])
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
                        #all_fields.add(k)
            else:
                # fallback: if pipeline_stats is a dict-like or empty, attempt best-effort extraction
                print(f"Warning: 'p_id' column not in {pipeline_stats.columns}")
                try:
                    pdata = pipeline_stats if isinstance(pipeline_stats, dict) else {}
                    for pid, p in pdata.items():
                        key_prefix = f'pipeline_{pid}_'
                        row[key_prefix + 'sum_tuples_mean'] = p.get('sum_tuples', 0)
                        row[key_prefix + 'sum_latency_mean'] = p.get('sum_time', 0)
                        row[key_prefix + 'comp_tp'] = p.get('comp_tp', 0)
                        row[key_prefix + 'eff_tp'] = p.get('eff_tp', 0)
                        #all_fields.update([key_prefix + k for k in ['sum_tuples_mean', 'sum_latency_mean', 'comp_tp', 'eff_tp']])
                except Exception:
                    pass

        csv_rows.append(row)
    if results_iter == []:
        print("No valid results to write to consolidated CSV.")
        # Ensure we have at least some fields in consistent order and write consolidated CSV
    out_csv_path = os.path.join(base_directory, os.path.basename(base_directory) + ".csv")
    if csv_rows:
        #all_fields = sorted(set().union(*(r.keys() for r in csv_rows)))
        pipeline_fields = []
        for p_id in range(1, 14):  # assuming max 13 pipelines
            pipeline_1_comp_tp_from_means = f'pipeline_{p_id}_comp_tp_from_means'
            pipeline_1_comp_tp_mean_of_runs = f'pipeline_{p_id}_comp_tp_mean_of_runs'
            pipeline_1_count_mean = f'pipeline_{p_id}_count_mean'
            pipeline_1_eff_tp_from_means = f'pipeline_{p_id}_eff_tp_from_means'
            pipeline_1_eff_tp_mean_of_runs = f'pipeline_{p_id}_eff_tp_mean_of_runs'
            pipeline_1_mean_latency = f'pipeline_{p_id}_mean_latency'
            pipeline_1_runs = f'pipeline_{p_id}_runs'
            pipeline_1_skipped_mean = f'pipeline_{p_id}_skipped_mean'
            pipeline_1_skipped_std = f'pipeline_{p_id}_skipped_std'
            pipeline_1_sum_latency_mean = f'pipeline_{p_id}_sum_latency_mean'
            pipeline_1_sum_tuples_mean = f'pipeline_{p_id}_sum_tuples_mean'
            pipeline_1_wall_time_mean = f'pipeline_{p_id}_wall_time_mean'
            pipeline_fields.extend([
                pipeline_1_comp_tp_from_means,
                pipeline_1_comp_tp_mean_of_runs,
                pipeline_1_count_mean,
                pipeline_1_eff_tp_from_means,
                pipeline_1_eff_tp_mean_of_runs,
                pipeline_1_mean_latency,
                pipeline_1_runs,
                pipeline_1_skipped_mean,
                pipeline_1_skipped_std,
                pipeline_1_sum_latency_mean,
                pipeline_1_sum_tuples_mean,
                pipeline_1_wall_time_mean
            ])
        all_fields = ["operator_chain", "layout", "buffer_size", "threads", "query_id", "test_id", "num_columns", "accessed_columns", "data_size", "selectivity", "individual_selectivity", "function_type", "aggregation_function", "id_data_type", "num_groups", "window_size",  "swap_strategy", "threshold", "total_tasks_time_mean", "total_tasks_time_std", "full_query_duration_mean", "full_query_duration_std"]  # add all possible fields here
        all_fields.extend(pipeline_fields)
    if os.path.exists(out_csv_path) and os.stat(out_csv_path).st_size > 0:
        mode = 'a'
    else:
        mode = 'w'
    if csv_rows:
        with open(out_csv_path, mode, newline='') as f:

            fieldnames = all_fields
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if mode =='w': #append, if new file write header
                writer.writeheader()

            for r in csv_rows:
                # fill missing fields with 0
                for fn in fieldnames:
                    if fn not in r:
                        r[fn] = 0
                writer.writerow(r)

        print(f"Consolidated CSV written to {out_csv_path}")









def main():

    # Get directory and optional trace files from command line
    parser = argparse.ArgumentParser(description='Process trace files')
    parser.add_argument('benchmark_directory', help='Directory to save results')
    parser.add_argument('--trace-files', nargs='+', help='List of trace files to process')
    parser.add_argument('--num-reps', type=int, default=2, help='Number of repetitions for averaging')
    parser.add_argument('--legacy', action='store_true', help='Use legacy processing method')
    args = parser.parse_args()

    legacy= False
    if args.legacy:
        legacy= True

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

    if legacy:
            # Process all trace files in parallel
            print("Using legacy processing method...")
            results = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
                results = list(executor.map(compute_stats, trace_files))

            query_config = get_config_from_file(Path(base_directory) / 'query_configs.csv')
            # Process data for CSV output
            csv_data = []
            all_fields = set(['filename', 'layout', 'buffer_size', 'threads', 'query_id',
                              'total_tasks_time', 'full_query_duration', 'total_skipped_tasks'])
            all_fields.update(query_config[next(iter(query_config))].keys())  # add config keys

            for file_path, total_time, full_time, pipelines, total_skipped in sorted(results):
                if isinstance(total_skipped, str) and total_skipped.startswith("Error"):
                    continue

                filename = os.path.basename(file_path)
                metadata = extract_metadata_from_filename(file_path)
                config = query_config.get(metadata.get('query_id', -1), {})
                row = {
                    #'filename': filename,
                    'layout': metadata.get('layout', ''),
                    #'buffer_size': metadata.get('buffer_size', ''),
                    'threads': metadata.get('threads', '4'),
                    'query_id': metadata.get('query', ''),
                    'total_tasks_time': total_time,
                    'full_query_duration': full_time,
                    'total_skipped_tasks': total_skipped
                }
                config.update(row)
                row = config.copy()

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
            csv_path = os.path.join(base_directory, os.path.basename(base_directory)+ "_old" + ".csv")
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
            print(f"legacy CSV written to {csv_path}")

    else:
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
            if not result or len(result) < 4:
                print("Warning: Incomplete result, skipping")
                continue
            windowed_stats, pipeline_stats, global_stats, metadata = result

            if metadata == {}:
                print("Warning: Missing metadata, skipping result")
                continue

            #print(f"Processing results for config: {metadata}")

            query_id = metadata['query_id']
            config = query_config[query_id]

            # Skip multi-operator chains if desired (keeps original behavior)
            operator_chain = config.get('operator_chain', ['unknown'])
            #if len(operator_chain) > 1:
                # still write per-config files but skip adding to consolidated CSV to mirror prior behavior
                #pass

            # Prepare buffer dir and write per-config files if DataFrames present
            try:
                #buffer_dir = Path(base_directory) / operator_chain[0] / f"bufferSize{metadata.get('buffer_size','')}"
                #buffer_dir.mkdir(parents=True, exist_ok=True)
                #result dir = query dir = parent /  parent / filename
                result_dir = Path(metadata['filename']).parent.parent
                #name= Path(metadata['filename']).stem.with_suffix('.csv')
                layout = metadata['layout']
                if windowed_stats is not None and not windowed_stats.empty:
                    windowed_stats.to_csv(result_dir / f"results_windowed_{layout}.csv", index=False)
                    #print(f"writing csv file to {result_dir / f"results_windowed_{layout}.csv"}")
                else:
                    print(f"Warning: empty windowed_stats for {metadata['filename']}")
                if pipeline_stats is not None and not windowed_stats.empty:
                    pipeline_stats.to_csv(result_dir / f"results_pipelined_{layout}.csv", index=False)
                else:
                    print(f"Warning: empty pipeline_stats for {metadata['filename']}", flush=True)
            except Exception as e:
                print(f"Warning: error writing per-config files: {e}", flush=True)

            config = query_config.get(metadata.get('query_id', -1), {})

            # Base row with global metrics
            row = {
                'layout': metadata.get('layout', ''),
                #'buffer_size': metadata.get('buffer_size', ''),
                'threads': metadata.get('threads', '4'),
                'query_id': metadata.get('query', ''),
                'total_tasks_time_mean': global_stats.get('total_tasks_time_mean', 0) if global_stats else 0,
                'total_tasks_time_std': global_stats.get('total_tasks_time_std', 0) if global_stats else 0,
                'full_query_duration_mean': global_stats.get('full_query_duration_mean', 0) if global_stats else 0,
                'full_query_duration_std': global_stats.get('full_query_duration_std', 0) if global_stats else 0,
                #'total_skipped_tasks': global_stats.get('total_skipped_tasks', 0) if global_stats else 0
            }

            config.update(row)

            row = config.copy()

            # Pull pipeline-level metrics from pipeline_stats DataFrame if present
            if pipeline_stats is not None and not pipeline_stats.empty:
                # ensure p_id column exists
                if 'p_id' in pipeline_stats.columns:
                    for _, prow in pipeline_stats.iterrows():
                        pid = int(prow['p_id'])
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
                    print(f"Warning: 'p_id' column not in {pipeline_stats.columns}")
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
        if results_iter == []:
            print("No valid results to write to consolidated CSV.")
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

        #remove old trace files (large)
        for trace_file in trace_files:
            try:
                os.remove(trace_file)
                #print(f"Removed trace file: {trace_file}")
            except Exception as e:
                print(f"Warning: Could not remove trace file {trace_file}: {e}")

if __name__ == "__main__":
    main()