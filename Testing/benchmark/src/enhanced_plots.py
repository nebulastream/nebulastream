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
        filename = f"{operator_type}_{param_name}_{metric.replace('pipeline_3_', '')}.png"
        plt.savefig(output_dir / filename)
        plt.close()

        #print(f"Created plot: {filename}")

def create_double_operator_plots(df, output_dir):
    """Create plots specifically for double operator benchmarks."""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Check if necessary columns exist
    required_cols = ['swap_strategy', 'operator_chain', 'buffer_size']
    if not all(col in df.columns for col in required_cols):
        print(f"Missing required columns. Found: {df.columns.tolist()}")
        return False

    # Create plots for each operator ID (3 and 5)
    operator_metrics = {
        3: ['pipeline_3_eff_tp', 'pipeline_3_comp_tp'],
        5: ['pipeline_5_eff_tp', 'pipeline_5_comp_tp']
    }

    # Group data by operator chain
    chain_groups = df.groupby('operator_chain')

    # 1. First create global plots across all chains
    create_global_plots(df, output_dir, operator_metrics)

    # 2. Create per-chain plots for each operator chain
    for chain_name, chain_df in chain_groups:
        chain_dir = output_dir / f"chain_{chain_name.replace(' ', '_').lower()}"
        chain_dir.mkdir(exist_ok=True, parents=True)

        # 2.1 Create plots per pipeline ID for this chain
        create_pipeline_plots(chain_df, chain_dir, operator_metrics)

        # 2.2 Create parameter-based plots for this chain
        create_parameter_plots(chain_df, chain_dir, operator_metrics)

    # 3. Create comparison plots between chains and strategies
    create_comparison_plots(df, output_dir, operator_metrics)

    return True

def create_global_plots(df, output_dir, operator_metrics):
    """Create global plots across all operator chains."""
    # Plot overall execution time by swap strategy and operator chain
    plt.figure(figsize=(14, 10))
    sns.barplot(
        data=df,
        x='operator_chain',
        y='full_query_duration',
        hue='swap_strategy',
        palette='viridis'
    )
    plt.title('Query Execution Time by Operator Chain and Strategy')
    plt.xlabel('Operator Chain')
    plt.ylabel('Execution Time (ms)')
    plt.yscale('log')
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(output_dir / "execution_time_by_chain_strategy.png")
    plt.close()

    # Plot throughput by buffer size for each pipeline ID
    for op_id, metrics in operator_metrics.items():
        for metric in metrics:
            if metric not in df.columns:
                continue

            metric_name = 'Effective' if 'eff_tp' in metric else 'Computational'

            plt.figure(figsize=(12, 8))
            sns.lineplot(
                data=df,
                x='buffer_size',
                y=metric,
                hue='swap_strategy',
                style='operator_chain',
                markers=True,
                palette='viridis'
            )
            plt.title(f'Pipeline {op_id}: {metric_name} Throughput by Buffer Size')
            plt.xlabel('Buffer Size')
            plt.ylabel(f'{metric_name} Throughput (tuples/s)')
            plt.xscale('log')
            plt.yscale('log')
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.tight_layout()
            plt.savefig(output_dir / f"op{op_id}_{metric_name.lower()}_throughput_global.png")
            plt.close()

def create_pipeline_plots(df, output_dir, operator_metrics):
    """Create plots specific to each pipeline in an operator chain."""
    # For each query (based on unique parameters), create a plot with both pipelines
    params_to_group = ['buffer_size', 'num_columns', 'accessed_columns']
    param_groups = df.groupby(params_to_group)

    for param_values, param_df in param_groups:
        param_str = '_'.join([f"{p}{v}" for p, v in zip(params_to_group, param_values)])

        # Plot comparing pipeline 3 and 5 in the same query
        plt.figure(figsize=(12, 8))
        metrics_to_plot = []
        for op_id in [3, 5]:
            metric = f'pipeline_{op_id}_eff_tp'
            if metric in param_df.columns:
                metrics_to_plot.append((op_id, metric))

        if metrics_to_plot:
            for op_id, metric in metrics_to_plot:
                sns.barplot(
                    data=param_df,
                    x='swap_strategy',
                    y=metric,
                    label=f'Pipeline {op_id}'
                )

            plt.title(f'Effective Throughput by Pipeline ID ({param_str})')
            plt.xlabel('Swap Strategy')
            plt.ylabel('Effective Throughput (tuples/s)')
            plt.yscale('log')
            plt.legend()
            plt.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()
            plt.savefig(output_dir / f"pipeline_comparison_{param_str}.png")
            plt.close()

def create_parameter_plots(df, output_dir, operator_metrics):
    """Create plots showing effect of different parameters similar to single operator plots."""
    # Parameters to analyze
    parameters = ['buffer_size', 'num_columns', 'accessed_columns']

    for param in parameters:
        if param not in df.columns:
            continue

        # For each pipeline ID and metric
        for op_id, metrics in operator_metrics.items():
            for metric in metrics:
                if metric not in df.columns:
                    continue

                metric_name = 'Effective' if 'eff_tp' in metric else 'Computational'

                # Plot by parameter, grouped by swap strategy
                plt.figure(figsize=(12, 8))

                # Group data and calculate means
                pivot_df = df.pivot_table(
                    index=param,
                    columns='swap_strategy',
                    values=metric,
                    aggfunc='mean'
                )

                if pivot_df.empty:
                    continue

                ax = pivot_df.plot(kind='bar', colormap='viridis')
                plt.title(f'Pipeline {op_id}: {metric_name} Throughput by {param}')
                plt.xlabel(param)
                plt.ylabel(f'{metric_name} Throughput (tuples/s)')
                plt.yscale('log')
                plt.grid(True, axis='y', linestyle='--', alpha=0.7)

                # Add value labels
                for container in ax.containers:
                    ax.bar_label(container, fmt='%.2e', rotation=90, padding=3)

                plt.tight_layout()
                plt.savefig(output_dir / f"op{op_id}_{metric_name.lower()}_throughput_by_{param}.png")
                plt.close()

def create_comparison_plots(df, output_dir, operator_metrics):
    """Create plots comparing different strategies and chains."""
    # Plot execution time by strategy for each chain
    for strategy in df['swap_strategy'].unique():
        strategy_df = df[df['swap_strategy'] == strategy]
        strategy_name = strategy.replace(' ', '_').lower()

        plt.figure(figsize=(14, 10))

        # Group by operator chain
        chain_times = strategy_df.groupby('operator_chain')['full_query_duration'].mean()
        plt.bar(chain_times.index, chain_times.values)

        plt.title(f'Strategy {strategy}: Execution Time Comparison')
        plt.xlabel('Operator Chain')
        plt.ylabel('Execution Time (ms)')
        plt.yscale('log')
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig(output_dir / f"strategy_{strategy_name}_execution_time.png")
        plt.close()

    # For each operator chain, compare throughput across different strategies
    for chain in df['operator_chain'].unique():
        chain_df = df[df['operator_chain'] == chain]
        chain_name = chain.replace(' ', '_').lower()

        # For each pipeline ID, plot throughput by strategy
        for op_id in [3, 5]:
            metric = f'pipeline_{op_id}_eff_tp'
            if metric not in chain_df.columns:
                continue

            plt.figure(figsize=(12, 8))
            sns.boxplot(
                data=chain_df,
                x='swap_strategy',
                y=metric
            )
            plt.title(f'{chain} - Pipeline {op_id}: Throughput by Strategy')
            plt.xlabel('Swap Strategy')
            plt.ylabel('Effective Throughput (tuples/s)')
            plt.yscale('log')
            plt.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()
            plt.savefig(output_dir / f"{chain_name}_op{op_id}_throughput_by_strategy.png")
            plt.close()

def create_filter_throughput_plots(df, output_dir):
    """Create plots for filter throughput with selectivity on x-axis."""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Filter data for single filter operations
    filter_df = df[df['operator_type'] == 'filter']

    if filter_df.empty:
        print("No filter data available for plotting.")
        return

    # Ensure required columns exist
    required_columns = ['selectivity', 'layout', 'num_columns', 'accessed_columns', 'pipeline_3_eff_tp', 'pipeline_3_comp_tp']
    if not all(col in filter_df.columns for col in required_columns):
        print("Missing required columns for filter throughput plots.")
        return

    # Metrics to plot
    metrics = [
        ('pipeline_3_eff_tp', 'Effective Throughput'),
        ('pipeline_3_comp_tp', 'Computational Throughput')
    ]

    # Group data by num_columns
    for num_cols in sorted(filter_df['num_columns'].unique()):
        num_cols_df = filter_df[filter_df['num_columns'] == num_cols]

        for metric, metric_label in metrics:
            plt.figure(figsize=(12, 8))
            ax = plt.gca()

            # Group by selectivity and layout
            grouped = num_cols_df.groupby(['selectivity', 'layout', 'accessed_columns'])[metric].mean().reset_index()

            # Pivot table for side-by-side bars
            pivot_df = grouped.pivot(index='selectivity', columns=['layout', 'accessed_columns'], values=metric)

            # Reorder columns to ensure ROW1 and COL1 are adjacent
            ordered_columns = sorted(pivot_df.columns, key=lambda x: (x[1], x[0]))
            pivot_df = pivot_df[ordered_columns]

            # Plot bars
            pivot_df.plot(kind='bar', ax=ax, width=0.8, colormap='viridis')

            # Format plot
            plt.title(f'Filter {metric_label} by Selectivity (Number of Total Columns: {num_cols})')
            plt.xlabel('Selectivity (%)')
            plt.ylabel(f'{metric_label} (tuples/s)')
            plt.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.tight_layout()

            # Save plot
            filename = f"filter_{metric.replace('pipeline_3_', '')}_num_cols_{num_cols}.png"
            plt.savefig(output_dir / filename)
            plt.close()

            print(f"Created plot: {filename}")

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

    # Check if this is a double operator benchmark
    is_double_op = False
    if 'is_double_op' in df.columns:
        is_double_op = df['is_double_op'].any()
    elif 'swap_strategy' in df.columns and 'operator_chain' in df.columns:
        is_double_op = True

    # Create appropriate plots based on benchmark type
    if is_double_op:
        if create_double_operator_plots(df, output_dir):
            print(f"Double operator plots created in {output_dir}")
    else:
        create_filter_throughput_plots(df, output_dir)
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