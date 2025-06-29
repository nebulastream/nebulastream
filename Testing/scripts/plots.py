#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import numpy as np
from pathlib import Path
import os
import shutil
from matplotlib.colors import to_rgba

# Define query descriptions
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

# Define color maps for buffer sizes
ROW_COLORS = {
    '4000': '#1f77b4',     # blue
    '400000': '#2a9df4',   # lighter blue
    '20000000': '#7fbfff'  # lightest blue
}

COL_COLORS = {
    '4000': '#ff7f0e',     # orange
    '400000': '#ffb14e',   # lighter orange
    '20000000': '#ffd699'  # lightest orange
}

# Define pipeline colors
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
    df = pd.read_csv(csv_file)
    # Ensure query is string type immediately
    df['query'] = df['query'].astype(str)
    return df

def get_query_label(query_id):
    """Get descriptive label for query ID."""
    query_id = str(query_id)  # Ensure string type
    if query_id in QUERY_DESCRIPTIONS:
        return f"Q{query_id}: {QUERY_DESCRIPTIONS[query_id]}"
    return f"Query {query_id}"

def scale_time_value(values):
    """Dynamically scale time values to appropriate units."""
    values = values.dropna()
    if len(values) == 0:
        return values, "s"

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
        return values, "s"

    # Calculate scaling
    data, unit = scale_time_value(values)

    # Calculate reasonable limits
    if len(data) > 0:
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

def create_query_directories(output_dir, df):
    """Create directories for each query."""
    query_dirs = {}
    for query in sorted(df['query'].unique()):
        query_str = str(query)  # Ensure it's a string
        query_dir = output_dir / f"query_{query_str}"
        query_dir.mkdir(exist_ok=True)
        query_dirs[query_str] = query_dir
    return query_dirs

def create_throughput_comparison_chart(df, output_dir, query_dirs):
    """Create charts comparing layout, buffer_size for each thread count."""
    # Convert types for clean comparison
    df = df.copy()  # Avoid SettingWithCopyWarning by creating explicit copy
    df['buffer_size'] = df['buffer_size'].astype(int)
    df['threads'] = df['threads'].astype(int)
    df['query'] = df['query'].astype(str)  # Ensure query is string
    df['query_label'] = df['query'].apply(get_query_label)

    # Create separate plots for each thread size
    for thread_count in sorted(df['threads'].unique()):
        thread_df = df[df['threads'] == thread_count].copy()

        # For each query, create a thread-specific plot
        for query in sorted(thread_df['query'].unique()):
            query_str = str(query)  # Ensure string type for dict lookup
            query_df = thread_df[thread_df['query'] == query].copy()

            # Skip if no data
            if query_df.empty or query_df['full_query_duration'].isna().all():
                continue

            query_label = get_query_label(query)

            plt.figure(figsize=(10, 6))
            ax = plt.gca()

            # Get unique buffer sizes
            buffer_sizes = sorted(query_df['buffer_size'].unique())

            # Set up positions for bars - now grouped by buffer_size
            bar_width = 0.35
            buffer_positions = np.arange(len(buffer_sizes)) * 3  # Allow space between buffer size groups
            bar_labels = []

            # Draw bars with ROW/COL of same buffer size next to each other
            for i, buffer_size in enumerate(buffer_sizes):
                buffer_str = str(buffer_size)
                buffer_df = query_df[query_df['buffer_size'] == buffer_size]

                # Position for this buffer size group
                row_pos = buffer_positions[i]
                col_pos = buffer_positions[i] + bar_width + 0.05

                # ROW layout bar
                row_data = buffer_df[buffer_df['layout'] == 'ROW_LAYOUT']
                if not row_data.empty and not row_data['full_query_duration'].isna().all():
                    row_mean = row_data['full_query_duration'].mean()
                    row_std = row_data['full_query_duration'].std()
                    ax.bar(row_pos, row_mean, width=bar_width,
                           color=ROW_COLORS.get(buffer_str, '#1f77b4'),
                           label=f"ROW {buffer_size}" if i == 0 else "_nolegend_")
                    if not np.isnan(row_std):
                        ax.errorbar(row_pos, row_mean, yerr=row_std, color='black', capsize=5)

                # COLUMNAR layout bar
                col_data = buffer_df[buffer_df['layout'] == 'COLUMNAR_LAYOUT']
                if not col_data.empty and not col_data['full_query_duration'].isna().all():
                    col_mean = col_data['full_query_duration'].mean()
                    col_std = col_data['full_query_duration'].std()
                    ax.bar(col_pos, col_mean, width=bar_width,
                           color=COL_COLORS.get(buffer_str, '#ff7f0e'),
                           label=f"COL {buffer_size}" if i == 0 else "_nolegend_")
                    if not np.isnan(col_std):
                        ax.errorbar(col_pos, col_mean, yerr=col_std, color='black', capsize=5)

                # Add group label centered between row and col
                bar_labels.append(f"{buffer_size}")

            # Only scale y-axis if we have data
            if not query_df['full_query_duration'].isna().all():
                data, unit = adjust_y_scale(ax, query_df['full_query_duration'])

            # Set styling
            plt.title(f'{query_label}: Query Duration by Layout ({thread_count} Threads)')
            plt.xlabel('Buffer Size')
            plt.ylabel(f'Full Query Duration ({unit if "unit" in locals() else "s"})')
            plt.xticks(buffer_positions + bar_width/2, bar_labels)
            plt.grid(True, axis='y', linestyle='--', alpha=0.7)

            # Create legend with all buffer sizes but grouped by layout
            handles, labels = [], []
            for bs in buffer_sizes:
                bs_str = str(bs)
                handles.append(plt.Rectangle((0,0),1,1, color=ROW_COLORS.get(bs_str, '#1f77b4')))
                labels.append(f"ROW {bs}")

            for bs in buffer_sizes:
                bs_str = str(bs)
                handles.append(plt.Rectangle((0,0),1,1, color=COL_COLORS.get(bs_str, '#ff7f0e')))
                labels.append(f"COL {bs}")

            plt.legend(handles, labels)
            plt.tight_layout()

            # Save to both main directory and query subfolder
            plt.savefig(os.path.join(output_dir, f'layout_vs_duration_query{query}_threads_{thread_count}.png'))
            plt.savefig(os.path.join(query_dirs[query_str], f'duration_threads_{thread_count}.png'))
            plt.close()

def create_pipeline_percentage_chart(df, output_dir, query_dirs):
    """Create charts showing percentage of time spent in each pipeline, split by query."""
    df = df.copy()  # Create explicit copy
    df['query'] = df['query'].astype(str)  # Ensure query is string

    pipeline_cols = [col for col in df.columns if 'pipeline_' in col and 'time_pct' in col]
    if not pipeline_cols:
        print("No pipeline time percentage columns found in the data.")
        return

    df['query_label'] = df['query'].apply(get_query_label)

    # Create separate plots for each query
    for query in sorted(df['query'].unique()):
        query_str = str(query)  # Ensure string type for dict lookup
        query_df = df[df['query'] == query].copy()
        query_label = get_query_label(query)

        # Skip if no data
        if query_df.empty:
            continue

        # Check if any pipeline data exists
        has_data = False
        for col in pipeline_cols:
            if not query_df[col].isna().all():
                has_data = True
                break

        if not has_data:
            continue

        plt.figure(figsize=(16, 10))
        ax = plt.gca()

        # Group by layout, buffer_size, threads for this query
        grouped = query_df.groupby(['layout', 'buffer_size', 'threads'])

        bar_positions = np.arange(len(grouped))
        bar_labels = []

        pipeline_id_mapping = {}
        for col in pipeline_cols:
            pipeline_id = col.split('_')[1]
            pipeline_id_mapping[col] = PIPELINE_COLORS.get(pipeline_id, f'C{len(pipeline_id_mapping) % 10}')

        for i, ((layout, buffer_size, threads), group) in enumerate(grouped):
            config_label = f"{layout}\n{buffer_size}, {threads}T"
            bar_labels.append(config_label)

            bottom = 0
            for col in sorted(pipeline_cols, key=lambda x: int(x.split('_')[1])):
                pipeline_id = col.split('_')[1]

                # Skip if column doesn't exist or all NaN
                if col not in group.columns or group[col].isna().all():
                    continue

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

        # Save to both main directory and query subfolder
        plt.savefig(os.path.join(output_dir, f'pipeline_percentages_query_{query}.png'))
        plt.savefig(os.path.join(query_dirs[query_str], f'pipeline_percentages.png'))
        plt.close()

def create_throughput_by_parameter_chart(df, output_dir, query_dirs, tp_type='eff'):
    """Create pipeline throughput comparison charts with special handling for pipeline 1."""
    df = df.copy()  # Create explicit copy
    df['query'] = df['query'].astype(str)  # Ensure query is string

    # Add TupleSizeInBytes handling for pipeline 1
    if 'TupleSizeInBytes' in df.columns and f'pipeline_1_{tp_type}_tp' in df.columns:
        df.loc[:, f'pipeline_1_{tp_type}_tuples_per_sec'] = df[f'pipeline_1_{tp_type}_tp'] / df['TupleSizeInBytes']

    df['query_label'] = df['query'].apply(get_query_label)

    # Get all pipeline throughput columns (excluding pipeline 1 bytes)
    pipeline_tp_cols = [col for col in df.columns if 'pipeline_' in col and f'{tp_type}_tp' in col
                        and not col.startswith(f'pipeline_1_{tp_type}_tp')]

    # Add converted pipeline 1 if available
    if f'pipeline_1_{tp_type}_tuples_per_sec' in df.columns:
        pipeline_tp_cols.append(f'pipeline_1_{tp_type}_tuples_per_sec')

    tp_type_label = 'Effective' if tp_type == 'eff' else 'Computational'

    # For each query, create thread-specific plots
    for query in sorted(df['query'].unique()):
        query_str = str(query)  # Ensure string type for dict lookup
        query_df = df[df['query'] == query].copy()
        query_label = get_query_label(query)

        # For each thread count, create comparison plots
        for thread_count in sorted(query_df['threads'].unique()):
            thread_df = query_df[query_df['threads'] == thread_count].copy()

            # Get relevant pipelines with data
            relevant_pipeline_cols = []
            for col in pipeline_tp_cols:
                if col in thread_df.columns and not thread_df[col].isna().all() and thread_df[col].max() > 0:
                    relevant_pipeline_cols.append(col)

            if not relevant_pipeline_cols:
                continue

            # Extract pipeline IDs
            pipeline_ids = []
            for col in relevant_pipeline_cols:
                if col == f'pipeline_1_{tp_type}_tuples_per_sec':
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

            # Position trackers
            x_positions = []
            x_labels = []
            pipeline_centers = []

            # For each pipeline, show comparison for each buffer size
            for p_idx, pipeline_id in enumerate(pipeline_ids):
                pipeline_start = p_idx * pipeline_spacing
                pipeline_centers.append(pipeline_start + pipeline_spacing/2)

                # Get column name
                if pipeline_id == 1 and f'pipeline_1_{tp_type}_tuples_per_sec' in relevant_pipeline_cols:
                    col_name = f'pipeline_1_{tp_type}_tuples_per_sec'
                    pipeline_label = "Pipeline 1 (tuples/s)"
                else:
                    col_name = f'pipeline_{pipeline_id}_{tp_type}_tp'
                    pipeline_label = f"Pipeline {pipeline_id}"

                # Draw bars for each buffer size
                for b_idx, buffer_size in enumerate(buffer_sizes):
                    buffer_df = thread_df[thread_df['buffer_size'] == buffer_size].copy()
                    buffer_str = str(buffer_size)

                    # Calculate positions
                    row_pos = pipeline_start + b_idx * 2 * (bar_width + group_spacing)
                    col_pos = row_pos + bar_width

                    # Draw ROW bar
                    row_df = buffer_df[buffer_df['layout'] == 'ROW_LAYOUT']
                    if not row_df.empty and col_name in row_df.columns and not row_df[col_name].isna().all():
                        row_value = row_df[col_name].mean()
                        row_bar = ax.bar(row_pos, row_value, width=bar_width,
                                         color=ROW_COLORS.get(buffer_str, '#1f77b4'),
                                         label=f'ROW {buffer_size}' if p_idx == 0 and b_idx == 0 else "")

                        # Value label
                        if not np.isnan(row_value) and row_value > 0:
                            ax.text(row_pos, row_value * 1.05, f"{row_value:.2e}",
                                    ha='center', va='bottom', rotation=90, size=8)

                    # Draw COLUMNAR bar
                    col_df = buffer_df[buffer_df['layout'] == 'COLUMNAR_LAYOUT']
                    if not col_df.empty and col_name in col_df.columns and not col_df[col_name].isna().all():
                        col_value = col_df[col_name].mean()
                        col_bar = ax.bar(col_pos, col_value, width=bar_width,
                                         color=COL_COLORS.get(buffer_str, '#ff7f0e'),
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
                if pipeline_centers.index(pos) + 1 < len(pipeline_centers):
                    ax.axvline(x=(pos + pipeline_centers[pipeline_centers.index(pos)+1])/2,
                               color='black', linestyle='--', alpha=0.3)

            # Set plot styling
            ax.set_yscale('log')  # Log scale for better visibility
            ax.grid(True, linestyle='--', alpha=0.7, axis='y')

            # Create proper legend with all buffer sizes
            handles, labels = [], []
            for bs in buffer_sizes:
                bs_str = str(bs)
                handles.append(plt.Rectangle((0,0),1,1, color=ROW_COLORS.get(bs_str, '#1f77b4')))
                labels.append(f"ROW {bs}")
                handles.append(plt.Rectangle((0,0),1,1, color=COL_COLORS.get(bs_str, '#ff7f0e')))
                labels.append(f"COL {bs}")

            plt.legend(handles, labels, loc='upper right')
            plt.title(f'{query_label}: Pipeline {tp_type_label} Throughput ({thread_count} Threads)')
            plt.ylabel(f'{tp_type_label} Throughput (tuples/s)')
            plt.xticks([])  # Hide default x-ticks
            plt.tight_layout()

            # Save to both main directory and query subfolder
            plt.savefig(os.path.join(output_dir, f'query{query}_{tp_type}_throughput_threads_{thread_count}.png'))
            plt.savefig(os.path.join(query_dirs[query_str], f'{tp_type}_throughput_threads_{thread_count}.png'))
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
    if output_dir.exists():
        shutil.rmtree(output_dir)  # Clean existing charts
    output_dir.mkdir(exist_ok=True)

    # Load data
    df = load_data(csv_file)

    # Create query subdirectories
    query_dirs = create_query_directories(output_dir, df)

    # Create charts
    create_throughput_comparison_chart(df, output_dir, query_dirs)
    create_pipeline_percentage_chart(df, output_dir, query_dirs)
    create_throughput_by_parameter_chart(df, output_dir, query_dirs, tp_type='eff')
    create_throughput_by_parameter_chart(df, output_dir, query_dirs, tp_type='comp')

    print(f"Charts created in directory: {output_dir}")

if __name__ == "__main__":
    main()