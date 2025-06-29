#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import numpy as np
from pathlib import Path
import os
from matplotlib.colors import to_rgba

# Define query descriptions (works with both '01' and '1' formats)
QUERY_DESCRIPTIONS = {
    '01': 'Filter 5% selectivity',
    '02': 'Filter 50% selectivity',
    '03': 'Filter 95% selectivity',
    '04': 'Map',
    '05': 'Filter and Map 5% selectivity',
    '06': 'Filter and Map 50% selectivity',
    '1': 'Filter 5% selectivity',
    '2': 'Filter 50% selectivity',
    '3': 'Filter 95% selectivity',
    '4': 'Map',
    '5': 'Filter and Map 5% selectivity',
    '6': 'Filter and Map 50% selectivity'
}

# Define a color map for pipelines to ensure consistency
PIPELINE_COLORS = {
    '1': '#1f77b4',  # blue
    '2': '#ff7f0e',  # orange
    '3': '#2ca02c',  # green
    '4': '#d62728',  # red
    '5': '#9467bd',  # purple
    '6': '#8c564b',  # brown
    '7': '#e377c2',  # pink
    '8': '#7f7f7f',  # gray
    '9': '#bcbd22',  # olive
    '10': '#17becf'  # teal
}

def load_data(csv_file):
    """Load data from CSV file."""
    return pd.read_csv(csv_file)

def get_query_label(query_id):
    """Get descriptive label for query ID."""
    if query_id in QUERY_DESCRIPTIONS:
        return f"Q{query_id}: {QUERY_DESCRIPTIONS[query_id]}"
    return f"Query {query_id}"

def scale_time_value(values):
    """Dynamically scale time values to appropriate units."""
    max_val = np.max(values)
    if max_val < 0.001:  # nanoseconds range
        return values * 1e9, "ns"
    elif max_val < 1:  # milliseconds range
        return values * 1e3, "ms"
    else:  # seconds range
        return values, "s"

def adjust_y_scale(ax, values):
    """Adjust y-scale to show small differences."""
    values = values.dropna()
    if len(values) == 0:
        return

    # Calculate scaling
    data, unit = scale_time_value(values)

    # Calculate reasonable limits
    min_val = np.min(data)
    max_val = np.max(data)

    # If values are very close, zoom in to show differences
    if max_val - min_val < max_val * 0.05:
        buffer = max_val * 0.1
        ax.set_ylim(min_val - buffer, max_val + buffer)

    # Update y-label with unit
    current_label = ax.get_ylabel()
    if "(" in current_label:
        base_label = current_label.split("(")[0].strip()
    else:
        base_label = current_label

    ax.set_ylabel(f"{base_label} ({unit})")

    return data, unit

def create_throughput_comparison_chart(df, output_dir):
    """Create charts comparing layout, buffer_size for each thread count."""
    # Convert types
    df['buffer_size'] = df['buffer_size'].astype(int)
    df['threads'] = df['threads'].astype(int)
    df['query'] = df['query'].astype(str)
    df['query_label'] = df['query'].apply(get_query_label)

    # Create separate plots for each thread size
    for thread_count in sorted(df['threads'].unique()):
        thread_df = df[df['threads'] == thread_count]

        # Create plot for this thread count
        plt.figure(figsize=(14, 8))
        ax = plt.gca()
        sns.boxplot(x='query_label', y='full_query_duration', hue='layout',
                    data=thread_df, ax=ax, palette={'ROW_LAYOUT': '#1f77b4', 'COLUMNAR_LAYOUT': '#ff7f0e'})

        # Scale y-axis
        data, unit = adjust_y_scale(ax, thread_df['full_query_duration'])
        thread_df.loc[:, 'scaled_duration'] = data  # Using .loc to fix the warning

        # Redraw with scaled data
        ax.clear()
        sns.boxplot(x='query_label', y='scaled_duration', hue='layout',
                    data=thread_df, ax=ax, palette={'ROW_LAYOUT': '#1f77b4', 'COLUMNAR_LAYOUT': '#ff7f0e'})

        plt.title(f'Query Duration by Layout and Query ({thread_count} Threads)')
        plt.xlabel('Query')
        plt.ylabel(f'Full Query Duration ({unit})')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'layout_vs_duration_threads_{thread_count}.png'))
        plt.close()

        # For each buffer size, compare layouts
        for buffer_size in sorted(thread_df['buffer_size'].unique()):
            buffer_df = thread_df[thread_df['buffer_size'] == buffer_size]

            plt.figure(figsize=(14, 8))
            ax = plt.gca()
            sns.boxplot(x='query_label', y='full_query_duration', hue='layout',
                        data=buffer_df, ax=ax, palette={'ROW_LAYOUT': '#1f77b4', 'COLUMNAR_LAYOUT': '#ff7f0e'})

            # Scale y-axis
            data, unit = adjust_y_scale(ax, buffer_df['full_query_duration'])
            buffer_df.loc[:, 'scaled_duration'] = data

            # Redraw with scaled data
            ax.clear()
            sns.boxplot(x='query_label', y='scaled_duration', hue='layout',
                        data=buffer_df, ax=ax, palette={'ROW_LAYOUT': '#1f77b4', 'COLUMNAR_LAYOUT': '#ff7f0e'})

            plt.title(f'Query Duration by Layout ({thread_count} Threads, {buffer_size} Buffer Size)')
            plt.xlabel('Query')
            plt.ylabel(f'Full Query Duration ({unit})')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, f'layout_vs_duration_threads_{thread_count}_buffer_{buffer_size}.png'))
            plt.close()

def create_pipeline_percentage_chart(df, output_dir):
    """Create charts showing percentage of time spent in each pipeline, split by query."""
    pipeline_cols = [col for col in df.columns if 'pipeline_' in col and 'time_pct' in col]
    if not pipeline_cols:
        print("No pipeline time percentage columns found in the data.")
        return

    df['query_label'] = df['query'].apply(get_query_label)

    # Create separate plots for each query
    for query in sorted(df['query'].unique()):
        query_df = df[df['query'] == query]
        query_label = get_query_label(query)

        plt.figure(figsize=(16, 10))
        ax = plt.gca()

        # Group by layout, buffer_size, threads for this query
        grouped = query_df.groupby(['layout', 'buffer_size', 'threads'])

        bar_positions = range(len(grouped))
        bar_labels = []

        pipeline_id_mapping = {}
        for col in pipeline_cols:
            pipeline_id = col.split('_')[1]
            pipeline_id_mapping[col] = PIPELINE_COLORS.get(pipeline_id, f'C{len(pipeline_id_mapping) % 10}')

        for i, ((layout, buffer_size, threads), group) in enumerate(grouped):
            config_label = f"{layout}\n{buffer_size}MB, {threads}T"
            bar_labels.append(config_label)

            bottom = 0
            for col in sorted(pipeline_cols, key=lambda x: int(x.split('_')[1])):
                pipeline_id = col.split('_')[1]
                value = group[col].mean() if not group[col].empty else 0

                color = pipeline_id_mapping[col]
                ax.bar(i, value, bottom=bottom, color=color,
                       label=f"Pipeline {pipeline_id}" if i == 0 else "")

                if value > 5:
                    ax.text(i, bottom + value/2, f"P{pipeline_id}\n{value:.1f}%",
                            ha='center', va='center', color='white', fontweight='bold')

                bottom += value

        plt.title(f'{query_label}: Percentage of Time Spent in Each Pipeline')
        plt.xlabel('Configuration')
        plt.ylabel('Percentage of Time (%)')
        plt.xticks(bar_positions, bar_labels, rotation=45, ha='right')
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        plt.legend(by_label.values(), by_label.keys(), title='Pipeline')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'pipeline_percentages_query_{query}.png'))
        plt.close()

def create_throughput_by_parameter_chart(df, output_dir):
    """Create pipeline throughput comparison charts with special handling for pipeline 1."""
    # Add TupleSizeInBytes handling for pipeline 1
    if 'TupleSizeInBytes' in df.columns and 'pipeline_1_eff_tp' in df.columns:
        df.loc[:, 'pipeline_1_tuples_per_sec'] = df['pipeline_1_eff_tp'] / df['TupleSizeInBytes']

    df['query_label'] = df['query'].apply(get_query_label)

    # Get all pipeline effective throughput columns (excluding pipeline 1 bytes)
    pipeline_tp_cols = [col for col in df.columns if 'pipeline_' in col and 'eff_tp' in col
                        and not col.startswith('pipeline_1_eff_tp')]

    # Add converted pipeline 1 if available
    if 'pipeline_1_tuples_per_sec' in df.columns:
        pipeline_tp_cols.append('pipeline_1_tuples_per_sec')

    # For each query, create thread-specific plots
    for query in sorted(df['query'].unique()):
        query_df = df[df['query'] == query]
        query_label = get_query_label(query)

        # For each thread count, create comparison plots
        for thread_count in sorted(query_df['threads'].unique()):
            thread_df = query_df[query_df['threads'] == thread_count]

            # Get relevant pipelines with data
            relevant_pipeline_cols = []
            for col in pipeline_tp_cols:
                if thread_df[col].notna().any() and thread_df[col].max() > 0:
                    relevant_pipeline_cols.append(col)

            if not relevant_pipeline_cols:
                continue

            # Extract pipeline IDs
            pipeline_ids = []
            for col in relevant_pipeline_cols:
                if col == 'pipeline_1_tuples_per_sec':
                    pipeline_ids.append(1)
                else:
                    pipeline_ids.append(int(col.split('_')[1]))
            pipeline_ids = sorted(list(set(pipeline_ids)))

            # Create comparison bar chart with color gradients
            plt.figure(figsize=(16, 10))
            ax = plt.gca()

            buffer_sizes = sorted(thread_df['buffer_size'].unique())
            n_buffers = len(buffer_sizes)

            # Set up spacing for grouped bars
            bar_width = 0.35
            group_spacing = 0.2
            pipeline_spacing = n_buffers * 2 * (bar_width + group_spacing) + 1

            # Create color gradients
            row_colors = [to_rgba('#1f77b4', alpha=0.5 + 0.5 * i / max(1, n_buffers-1))
                          for i in range(n_buffers)]
            col_colors = [to_rgba('#ff7f0e', alpha=0.5 + 0.5 * i / max(1, n_buffers-1))
                          for i in range(n_buffers)]

            # Position trackers
            x_positions = []
            x_labels = []
            pipeline_centers = []

            # For each pipeline, show comparison for each buffer size
            for p_idx, pipeline_id in enumerate(pipeline_ids):
                pipeline_start = p_idx * pipeline_spacing
                pipeline_centers.append(pipeline_start + pipeline_spacing/2)

                # Get column name
                if pipeline_id == 1 and 'pipeline_1_tuples_per_sec' in relevant_pipeline_cols:
                    col_name = 'pipeline_1_tuples_per_sec'
                    pipeline_label = "Pipeline 1 (tuples/s)"
                else:
                    col_name = f'pipeline_{pipeline_id}_eff_tp'
                    pipeline_label = f"Pipeline {pipeline_id}"

                # Draw bars for each buffer size
                for b_idx, buffer_size in enumerate(buffer_sizes):
                    buffer_df = thread_df[thread_df['buffer_size'] == buffer_size]

                    # Calculate positions
                    row_pos = pipeline_start + b_idx * 2 * (bar_width + group_spacing)
                    col_pos = row_pos + bar_width

                    # Draw ROW bar
                    row_df = buffer_df[buffer_df['layout'] == 'ROW_LAYOUT']
                    if not row_df.empty and col_name in row_df.columns:
                        row_value = row_df[col_name].mean()
                        row_bar = ax.bar(row_pos, row_value, width=bar_width,
                                         color=row_colors[b_idx],
                                         label=f'ROW {buffer_size}' if p_idx == 0 and b_idx == 0 else "")

                        # Value label
                        if not np.isnan(row_value) and row_value > 0:
                            ax.text(row_pos, row_value * 1.05, f"{row_value:.2e}",
                                    ha='center', va='bottom', rotation=90, size=8)

                    # Draw COLUMNAR bar
                    col_df = buffer_df[buffer_df['layout'] == 'COLUMNAR_LAYOUT']
                    if not col_df.empty and col_name in col_df.columns:
                        col_value = col_df[col_name].mean()
                        col_bar = ax.bar(col_pos, col_value, width=bar_width,
                                         color=col_colors[b_idx],
                                         label=f'COL {buffer_size}' if p_idx == 0 and b_idx == 0 else "")

                        # Value label
                        if not np.isnan(col_value) and col_value > 0:
                            ax.text(col_pos, col_value * 1.05, f"{col_value:.2e}",
                                    ha='center', va='bottom', rotation=90, size=8)

                    # Track position for labels
                    if p_idx == 0:
                        x_positions.append((row_pos + col_pos) / 2)
                        x_labels.append(f"Buffer\n{buffer_size}")

                # Add pipeline label
                ax.text(pipeline_start + pipeline_spacing/2, ax.get_ylim()[1] * 0.02,
                        pipeline_label, ha='center', va='bottom', fontweight='bold',
                        bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.3'))

            # Add pipeline separators
            for pos in pipeline_centers[:-1]:
                ax.axvline(x=(pos + pipeline_centers[pipeline_centers.index(pos)+1])/2,
                           color='black', linestyle='--', alpha=0.3)

            # Set plot styling
            ax.set_yscale('log')  # Log scale for better visibility
            ax.grid(True, linestyle='--', alpha=0.7, axis='y')

            plt.title(f'{query_label}: Pipeline Throughput Comparison ({thread_count} Threads)')
            plt.ylabel('Throughput (tuples/s)')
            plt.xticks([])  # Hide default x-ticks
            plt.legend(loc='upper right')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, f'query{query}_pipeline_comparison_threads_{thread_count}.png'))
            plt.close()

def create_computational_throughput_chart(df, output_dir):
    """Create pipeline computational throughput comparison charts with special handling for pipeline 1."""
    # Add TupleSizeInBytes handling for pipeline 1
    if 'TupleSizeInBytes' in df.columns and 'pipeline_1_comp_tp' in df.columns:
        df.loc[:, 'pipeline_1_comp_tuples_per_sec'] = df['pipeline_1_comp_tp'] / df['TupleSizeInBytes']

    df['query_label'] = df['query'].apply(get_query_label)

    # Get all pipeline computational throughput columns (excluding pipeline 1 bytes)
    pipeline_tp_cols = [col for col in df.columns if 'pipeline_' in col and 'comp_tp' in col
                        and not col.startswith('pipeline_1_comp_tp')]

    # Add converted pipeline 1 if available
    if 'pipeline_1_comp_tuples_per_sec' in df.columns:
        pipeline_tp_cols.append('pipeline_1_comp_tuples_per_sec')

    # For each query, create thread-specific plots
    for query in sorted(df['query'].unique()):
        query_df = df[df['query'] == query]
        query_label = get_query_label(query)

        # For each thread count, create comparison plots
        for thread_count in sorted(query_df['threads'].unique()):
            thread_df = query_df[query_df['threads'] == thread_count]

            # Get relevant pipelines with data
            relevant_pipeline_cols = []
            for col in pipeline_tp_cols:
                if thread_df[col].notna().any() and thread_df[col].max() > 0:
                    relevant_pipeline_cols.append(col)

            if not relevant_pipeline_cols:
                continue

            # Extract pipeline IDs
            pipeline_ids = []
            for col in relevant_pipeline_cols:
                if col == 'pipeline_1_comp_tuples_per_sec':
                    pipeline_ids.append(1)
                else:
                    pipeline_ids.append(int(col.split('_')[1]))
            pipeline_ids = sorted(list(set(pipeline_ids)))

            # Create comparison bar chart with color gradients
            plt.figure(figsize=(16, 10))
            ax = plt.gca()

            buffer_sizes = sorted(thread_df['buffer_size'].unique())
            n_buffers = len(buffer_sizes)

            # Set up spacing for grouped bars
            bar_width = 0.35
            group_spacing = 0.2
            pipeline_spacing = n_buffers * 2 * (bar_width + group_spacing) + 1

            # Create color gradients
            row_colors = [to_rgba('#1f77b4', alpha=0.5 + 0.5 * i / max(1, n_buffers-1))
                          for i in range(n_buffers)]
            col_colors = [to_rgba('#ff7f0e', alpha=0.5 + 0.5 * i / max(1, n_buffers-1))
                          for i in range(n_buffers)]

            # Position trackers
            x_positions = []
            x_labels = []
            pipeline_centers = []

            # For each pipeline, show comparison for each buffer size
            for p_idx, pipeline_id in enumerate(pipeline_ids):
                pipeline_start = p_idx * pipeline_spacing
                pipeline_centers.append(pipeline_start + pipeline_spacing/2)

                # Get column name
                if pipeline_id == 1 and 'pipeline_1_comp_tuples_per_sec' in relevant_pipeline_cols:
                    col_name = 'pipeline_1_comp_tuples_per_sec'
                    pipeline_label = "Pipeline 1 (tuples/s)"
                else:
                    col_name = f'pipeline_{pipeline_id}_comp_tp'
                    pipeline_label = f"Pipeline {pipeline_id}"

                # Draw bars for each buffer size
                for b_idx, buffer_size in enumerate(buffer_sizes):
                    buffer_df = thread_df[thread_df['buffer_size'] == buffer_size]

                    # Calculate positions
                    row_pos = pipeline_start + b_idx * 2 * (bar_width + group_spacing)
                    col_pos = row_pos + bar_width

                    # Draw ROW bar
                    row_df = buffer_df[buffer_df['layout'] == 'ROW_LAYOUT']
                    if not row_df.empty and col_name in row_df.columns:
                        row_value = row_df[col_name].mean()
                        row_bar = ax.bar(row_pos, row_value, width=bar_width,
                                         color=row_colors[b_idx],
                                         label=f'ROW {buffer_size}' if p_idx == 0 and b_idx == 0 else "")

                        # Value label
                        if not np.isnan(row_value) and row_value > 0:
                            ax.text(row_pos, row_value * 1.05, f"{row_value:.2e}",
                                    ha='center', va='bottom', rotation=90, size=8)

                    # Draw COLUMNAR bar
                    col_df = buffer_df[buffer_df['layout'] == 'COLUMNAR_LAYOUT']
                    if not col_df.empty and col_name in col_df.columns:
                        col_value = col_df[col_name].mean()
                        col_bar = ax.bar(col_pos, col_value, width=bar_width,
                                         color=col_colors[b_idx],
                                         label=f'COL {buffer_size}' if p_idx == 0 and b_idx == 0 else "")

                        # Value label
                        if not np.isnan(col_value) and col_value > 0:
                            ax.text(col_pos, col_value * 1.05, f"{col_value:.2e}",
                                    ha='center', va='bottom', rotation=90, size=8)

                    # Track position for labels
                    if p_idx == 0:
                        x_positions.append((row_pos + col_pos) / 2)
                        x_labels.append(f"Buffer\n{buffer_size}")

                # Add pipeline label
                ax.text(pipeline_start + pipeline_spacing/2, ax.get_ylim()[1] * 0.02,
                        pipeline_label, ha='center', va='bottom', fontweight='bold',
                        bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.3'))

            # Add pipeline separators
            for pos in pipeline_centers[:-1]:
                ax.axvline(x=(pos + pipeline_centers[pipeline_centers.index(pos)+1])/2,
                           color='black', linestyle='--', alpha=0.3)

            # Set plot styling
            ax.set_yscale('log')  # Log scale for better visibility
            ax.grid(True, linestyle='--', alpha=0.7, axis='y')

            plt.title(f'{query_label}: Pipeline Computational Throughput ({thread_count} Threads)')
            plt.ylabel('Computational Throughput (tuples/s)')
            plt.xticks([])  # Hide default x-ticks
            plt.legend(loc='upper right')
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, f'query{query}_comp_throughput_threads_{thread_count}.png'))
            plt.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python plots.py <path_to_csv_file>")
        sys.exit(1)

    csv_file = Path(sys.argv[1])
    if not csv_file.exists():
        print(f"CSV file not found: {csv_file}")
        sys.exit(1)

    # Create output directory for charts
    output_dir = csv_file.parent / "charts"
    output_dir.mkdir(exist_ok=True)

    # Load data
    df = load_data(csv_file)

    # Create charts
    create_throughput_comparison_chart(df, output_dir)
    create_pipeline_percentage_chart(df, output_dir)
    create_throughput_by_parameter_chart(df, output_dir)
    create_computational_throughput_chart(df, output_dir)
    print(f"Charts created in directory: {output_dir}")

if __name__ == "__main__":
    main()