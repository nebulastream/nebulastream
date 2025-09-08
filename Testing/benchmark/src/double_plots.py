#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
from pathlib import Path

def load_double_operator_results(benchmark_dir):
    """Load double operator results from benchmark directory."""
    benchmark_dir = Path(benchmark_dir)

    # Try dedicated double operator results file first
    double_results = benchmark_dir / f"{benchmark_dir.name}_double_operator_results.csv"
    if double_results.exists():
        return pd.read_csv(double_results)

    # Try the combined results file filtering for double operators
    combined_results = benchmark_dir / f"{benchmark_dir.name}_avg_results.csv"
    if combined_results.exists():
        df = pd.read_csv(combined_results)
        if 'is_double_op' in df.columns:
            return df[df['is_double_op'] == True]

    # Look for results in the double_operator directory structure
    double_op_dir = benchmark_dir / "double_operator"
    if not double_op_dir.exists():
        return None

    # Collect and combine all avg_results.csv files
    dfs = []
    for strategy_dir in double_op_dir.glob("*"):
        if not strategy_dir.is_dir():
            continue

        for buffer_dir in strategy_dir.glob("bufferSize*"):
            for query_dir in buffer_dir.glob("query_*"):
                results_file = query_dir / "avg_results.csv"
                if results_file.exists():
                    df = pd.read_csv(results_file)
                    df['strategy'] = strategy_dir.name
                    df['buffer_size'] = int(buffer_dir.name.replace("bufferSize", ""))

                    # Extract operator chain from directory name
                    query_name = query_dir.name
                    if "query_" in query_name and "_cols" in query_name:
                        chain = query_name.split("query_")[1].split("_cols")[0]
                        df['operator_chain'] = chain

                    dfs.append(df)

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return None

def plot_swap_strategy_comparison(df, output_dir):
    """Create plots comparing different swap strategies."""
    if df is None or df.empty:
        print("No data available for plotting swap strategies")
        return

    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Convert operator_chain to string if it's not already
    if 'operator_chain' in df.columns:
        df['operator_chain'] = df['operator_chain'].astype(str)

    # Ensure we have the required columns
    required_cols = ['buffer_size', 'layout', 'operator_chain', 'swap_strategy']
    metrics = [
        'pipeline_3_eff_tp', 'pipeline_3_comp_tp',
        'full_query_duration', 'swap_penalty_ms', 'swap_penalty_ratio'
    ]

    # Filter for available columns
    plot_metrics = [col for col in metrics if col in df.columns]
    if not all(col in df.columns for col in required_cols) or not plot_metrics:
        print(f"Required columns missing in data. Available: {df.columns.tolist()}")
        return

    # Plot 1: Swap Strategy Comparison for Throughput
    plt.figure(figsize=(15, 10))
    g = sns.catplot(
        data=df,
        x='buffer_size',
        y='pipeline_3_eff_tp',
        hue='swap_strategy',
        col='operator_chain',
        row='layout',
        kind='bar',
        height=4,
        aspect=1.5,
        palette='viridis'
    )
    g.set_axis_labels("Buffer Size", "Effective Throughput (tuples/s)")
    g.set_titles("{col_name} - {row_name}")
    g.fig.suptitle("Throughput by Swap Strategy", fontsize=16)
    plt.tight_layout()
    plt.subplots_adjust(top=0.9)
    plt.savefig(output_dir / "swap_strategy_throughput.png")
    plt.close()

    # Plot 2: Swap Penalty Comparison
    if 'swap_penalty_ms' in df.columns:
        plt.figure(figsize=(15, 10))
        g = sns.catplot(
            data=df,
            x='buffer_size',
            y='swap_penalty_ms',
            hue='swap_strategy',
            col='operator_chain',
            row='layout',
            kind='bar',
            height=4,
            aspect=1.5,
            palette='rocket'
        )
        g.set_axis_labels("Buffer Size", "Swap Overhead (ms)")
        g.set_titles("{col_name} - {row_name}")
        g.fig.suptitle("Swap Overhead by Strategy", fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        plt.savefig(output_dir / "swap_overhead.png")
        plt.close()

    # Plot 3: Full Query Duration
    if 'full_query_duration' in df.columns:
        plt.figure(figsize=(15, 10))
        g = sns.catplot(
            data=df,
            x='buffer_size',
            y='full_query_duration',
            hue='swap_strategy',
            col='operator_chain',
            row='layout',
            kind='bar',
            height=4,
            aspect=1.5,
            palette='mako'
        )
        g.set_axis_labels("Buffer Size", "Query Duration (ms)")
        g.set_titles("{col_name} - {row_name}")
        g.fig.suptitle("Full Query Duration by Strategy", fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        plt.savefig(output_dir / "query_duration.png")
        plt.close()

    # Plot 4: Operator Chain Performance Comparison
    plt.figure(figsize=(12, 8))
    sns.barplot(
        data=df,
        x='operator_chain',
        y='pipeline_3_eff_tp',
        hue='layout',
        palette='Set2'
    )
    plt.title('Performance by Operator Chain')
    plt.xlabel('Operator Chain')
    plt.ylabel('Effective Throughput (tuples/s)')
    plt.yscale('log')
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(output_dir / "operator_chain_comparison.png")
    plt.close()

    # Plot 5: Heatmap of relative performance between strategies
    pivot = df.pivot_table(
        values='pipeline_3_eff_tp',
        index=['buffer_size', 'operator_chain'],
        columns=['swap_strategy', 'layout']
    )

    if not pivot.empty:
        # Create heatmap of strategy performance
        plt.figure(figsize=(15, 10))
        sns.heatmap(
            pivot,
            annot=True,
            cmap='RdYlGn',
            fmt='.2e'
        )
        plt.title('Throughput Heatmap by Strategy')
        plt.tight_layout()
        plt.savefig(output_dir / "strategy_throughput_heatmap.png")
        plt.close()

    # Plot 6: Swap Penalty Ratio by Strategy (if available)
    if 'swap_penalty_ratio' in df.columns:
        plt.figure(figsize=(15, 10))
        g = sns.catplot(
            data=df,
            x='buffer_size',
            y='swap_penalty_ratio',
            hue='swap_strategy',
            col='operator_chain',
            row='layout',
            kind='bar',
            height=4,
            aspect=1.5,
            palette='flare'
        )
        g.set_axis_labels("Buffer Size", "Swap Penalty (ratio)")
        g.set_titles("{col_name} - {row_name}")
        g.fig.suptitle("Swap Penalty Ratio by Strategy", fontsize=16)
        plt.tight_layout()
        plt.subplots_adjust(top=0.9)
        plt.savefig(output_dir / "swap_penalty_ratio.png")
        plt.close()

def main():
    parser = argparse.ArgumentParser(description='Process and visualize double operator benchmarks')
    parser.add_argument('--benchmark-dir', required=True, help='Benchmark directory')
    parser.add_argument('--output-dir', help='Output directory for plots')
    args = parser.parse_args()

    benchmark_dir = Path(args.benchmark_dir)
    output_dir = Path(args.output_dir) if args.output_dir else benchmark_dir / "double_operator" / "plots"
    output_dir.mkdir(exist_ok=True, parents=True)

    # Load data
    df = load_double_operator_results(benchmark_dir)
    if df is None or df.empty:
        print("No double operator benchmark data found")
        return

    # Create plots
    plot_swap_strategy_comparison(df, output_dir)
    print(f"Double operator plots created in {output_dir}")

if __name__ == "__main__":
    main()