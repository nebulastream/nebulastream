#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
from pathlib import Path
import os

def create_layout_comparison_plots(df, output_dir):
    """Create plots comparing ROW vs COLUMNAR layouts for different parameters."""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Separate filter and map operations
    filter_df = df[df['operator_type'] == 'filter']
    map_df = df[df['operator_type'] == 'map']

    # Plot throughput by buffer size for each operator type
    plot_by_param(filter_df, output_dir, 'buffer_size', 'Buffer Size', log_scale=True)
    plot_by_param(map_df, output_dir, 'buffer_size', 'Buffer Size', log_scale=True)

    # For filter operations, also plot by selectivity
    if not filter_df.empty and 'selectivity' in filter_df.columns:
        plot_by_param(filter_df, output_dir, 'selectivity', 'Selectivity (%)')

    # For map operations, also plot by function type
    if not map_df.empty and 'function_type' in map_df.columns:
        plot_by_param(map_df, output_dir, 'function_type', 'Function Type')

    # Plot by number of columns and accessed columns for both operators
    if 'num_columns' in df.columns:
        plot_by_param(filter_df, output_dir, 'num_columns', 'Number of Columns')
        plot_by_param(map_df, output_dir, 'num_columns', 'Number of Columns')

    if 'accessed_columns' in df.columns:
        plot_by_param(filter_df, output_dir, 'accessed_columns', 'Accessed Columns')
        plot_by_param(map_df, output_dir, 'accessed_columns', 'Accessed Columns')

def plot_by_param(df, output_dir, param_name, param_label, log_scale=False):
    """Create throughput comparison plots for a specific parameter."""
    if df.empty or param_name not in df.columns:
        return

    operator_type = df['operator_type'].iloc[0]

    # Metrics to plot
    metrics = [
        ('pipeline_3_eff_tp', 'Effective Throughput'),
        ('pipeline_3_comp_tp', 'Computational Throughput')
    ]

    for metric, metric_label in metrics:
        if metric not in df.columns:
            continue

        plt.figure(figsize=(12, 8))

        # Group by parameter and layout
        grouped = df.groupby([param_name, 'layout'])[metric].mean().reset_index()

        # Pivot table for side-by-side bars
        pivot_df = grouped.pivot(index=param_name, columns='layout', values=metric)

        # Plot bars
        ax = pivot_df.plot(kind='bar', color=['#1f77b4', '#ff7f0e'])

        # Format plot
        plt.title(f'{operator_type.capitalize()}: {metric_label} by {param_label}')
        plt.xlabel(param_label)
        plt.ylabel(f'{metric_label} (tuples/s)')
        if log_scale:
            plt.yscale('log')
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()

        # Add value labels
        for container in ax.containers:
            ax.bar_label(container, fmt='%.2e', rotation=90, padding=3)

        # Save plot
        filename = f"{operator_type}_{param_name}_{metric.replace('pipeline_1_', '')}.png"
        plt.savefig(output_dir / filename)
        plt.close()

        #print(f"Created plot: {filename}")

def main():
    parser = argparse.ArgumentParser(description='Create enhanced benchmark plots')
    parser.add_argument('--results-csv', required=True, help='Path to benchmark results CSV')
    parser.add_argument('--output-dir', help='Output directory for plots')
    args = parser.parse_args()

    results_csv = Path(args.results_csv)
    if not results_csv.exists():
        print(f"Error: Results CSV file not found: {results_csv}")
        return

    # Default output directory is charts/ under the CSV directory
    output_dir = args.output_dir
    if not output_dir:
        output_dir = results_csv.parent / "charts"

    # Load data
    df = pd.read_csv(results_csv)

    # Ensure layout is a string
    if 'layout' in df.columns:
        df['layout'] = df['layout'].astype(str)

    # Create plots
    create_layout_comparison_plots(df, output_dir)

    # Additionally call the existing plots.py script
    #try:
    #   subprocess.run(["python3", "Testing/scripts/plots.py", str(results_csv)], check=True)
    #   print(f"Additional plots created using existing plots.py script")
    #except Exception as e:
    #   print(f"Error running existing plots.py: {e}")

    print(f"All plots saved to {output_dir}")

if __name__ == "__main__":
    main()