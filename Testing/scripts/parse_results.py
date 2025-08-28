#!/usr/bin/env python3
import re
import csv
import os
import sys
from pathlib import Path

def extract_params_from_filename(filename):
    """Extract parameters from the GoogleEventTrace filename."""
    pattern = r'GoogleEventTrace_([A-Z_]+)_buffer(\d+)_threads(\d+)_query(\d+)'
    match = re.search(pattern, filename)
    if match:
        layout, buffer_size, threads, query = match.groups()
        return {
            'layout': layout,
            'buffer_size': buffer_size,
            'threads': threads,
            'query': query
        }
    return {}

def parse_results_file(results_file):
    """Parse the results.txt file and extract metrics."""
    data = []
    current_entry = None

    with open(results_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()

        # Start of a new section
        if line.startswith('File:'):
            if current_entry:
                data.append(current_entry)

            filename = line.split('File: ')[1]
            params = extract_params_from_filename(filename)

            current_entry = {
                'filename': filename,
                **params,
                'pipelines': {}
            }

        # Extract total metrics
        elif line.startswith('Total tasks time:'):
            current_entry['total_tasks_time'] = float(line.split('Total tasks time: ')[1].replace('s', ''))
        elif line.startswith('Full query duration:'):
            current_entry['full_query_duration'] = float(line.split('Full query duration: ')[1].replace('s', ''))
        elif line.startswith('Total skipped tasks:'):
            current_entry['total_skipped_tasks'] = int(line.split('Total skipped tasks: ')[1])

        # Extract pipeline metrics
        elif line.startswith('Pipeline '):
            pattern = r'Pipeline (\d+): comp_tp=([\d.]+) tuples/s, eff_tp=([\d.]+) tuples/s, total_tuples=(\d+), time_pct=([\d.]+)%, skipped_tasks=(\d+)'
            match = re.search(pattern, line)
            if match:
                pipeline_id, comp_tp, eff_tp, total_tuples, time_pct, skipped_tasks = match.groups()
                current_entry['pipelines'][pipeline_id] = {
                    'comp_tp': float(comp_tp),
                    'eff_tp': float(eff_tp),
                    'total_tuples': int(total_tuples),
                    'time_pct': float(time_pct),
                    'skipped_tasks': int(skipped_tasks)
                }

    # Add the last entry
    if current_entry:
        data.append(current_entry)

    return data

def write_to_csv(data, output_file):
    """Write the parsed data to a CSV file."""
    if not data:
        print("No data to write to CSV.")
        return

    # Determine all pipeline IDs present in the data
    all_pipeline_ids = set()
    for entry in data:
        all_pipeline_ids.update(entry['pipelines'].keys())

    all_pipeline_ids = sorted(all_pipeline_ids, key=int)

    # Prepare headers
    headers = ['filename', 'layout', 'buffer_size', 'threads', 'query',
               'total_tasks_time', 'full_query_duration', 'total_skipped_tasks']

    for pipeline_id in all_pipeline_ids:
        for metric in ['comp_tp', 'eff_tp', 'total_tuples', 'time_pct', 'skipped_tasks']:
            headers.append(f'pipeline_{pipeline_id}_{metric}')

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for entry in data:
            row = {
                'filename': entry.get('filename', ''),
                'layout': entry.get('layout', ''),
                'buffer_size': entry.get('buffer_size', ''),
                'threads': entry.get('threads', ''),
                'query': entry.get('query', ''),
                'total_tasks_time': entry.get('total_tasks_time', ''),
                'full_query_duration': entry.get('full_query_duration', ''),
                'total_skipped_tasks': entry.get('total_skipped_tasks', '')
            }

            # Add pipeline metrics
            for pipeline_id in all_pipeline_ids:
                pipeline_data = entry['pipelines'].get(pipeline_id, {})
                for metric in ['comp_tp', 'eff_tp', 'total_tuples', 'time_pct', 'skipped_tasks']:
                    row[f'pipeline_{pipeline_id}_{metric}'] = pipeline_data.get(metric, '')

            writer.writerow(row)

    print(f"CSV file created: {output_file}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_results.py <path_to_benchmark_dir>")
        sys.exit(1)

    benchmark_dir = Path(sys.argv[1])
    results_file = benchmark_dir / "results.txt"

    if not results_file.exists():
        print(f"Results file not found: {results_file}")
        sys.exit(1)

    # Extract benchmark ID from directory name
    benchmark_id = re.search(r'benchmark(\d+)', str(benchmark_dir))
    if benchmark_id:
        benchmark_id = benchmark_id.group(1)
    else:
        benchmark_id = "unknown"

    csv_file = benchmark_dir / f"benchmark{benchmark_id}.csv"

    data = parse_results_file(results_file)
    write_to_csv(data, csv_file)

if __name__ == "__main__":
    main()