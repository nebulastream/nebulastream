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
    """Create charts comparing layout, buffer_size, and threads to throughput."""
    # Convert types
    df['buffer_size'] = df['buffer_size'].astype(int)
    df['threads'] = df['threads'].astype(int)
    df['query'] = df['query'].astype(str)

    # Add descriptive query labels
    df['query_label'] = df['query'].apply(get_query_label)

    # Compare layout vs duration by query
    plt.figure(figsize=(14, 8))
    ax = plt.gca()

    # Create plot
    sns.boxplot(x='query_label', y='full_query_duration', hue='layout', data=df, ax=ax)

    # Scale y-axis
    data, unit = adjust_y_scale(ax, df['full_query_duration'])
    df['scaled_duration'] = data

    # Update plot with scaled data
    ax.clear()
    sns.boxplot(x='query_label', y='scaled_duration', hue='layout', data=df, ax=ax)

    plt.title('Query Duration by Layout and Query')
    plt.xlabel('Query')
    plt.ylabel(f'Full Query Duration ({unit})')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'layout_vs_duration.png'))
    plt.close()

    # Compare buffer size vs duration by query
    plt.figure(figsize=(14, 8))
    ax = plt.gca()
    sns.boxplot(x='query_label', y='full_query_duration', hue='buffer_size', data=df, ax=ax)
    adjust_y_scale(ax, df['full_query_duration'])
    plt.title('Query Duration by Buffer Size and Query')
    plt.xlabel('Query')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'buffer_vs_duration.png'))
    plt.close()

    # Compare threads vs duration by query
    plt.figure(figsize=(14, 8))
    ax = plt.gca()
    sns.boxplot(x='query_label', y='full_query_duration', hue='threads', data=df, ax=ax)
    adjust_y_scale(ax, df['full_query_duration'])
    plt.title('Query Duration by Threads and Query')
    plt.xlabel('Query')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'threads_vs_duration.png'))
    plt.close()

def create_pipeline_percentage_chart(df, output_dir):
    """Create charts showing percentage of time spent in each pipeline."""
    # Get all pipeline time percentage columns
    pipeline_cols = [col for col in df.columns if 'pipeline_' in col and 'time_pct' in col]

    if not pipeline_cols:
        print("No pipeline time percentage columns found in the data.")
        return

    # Prepare data for stacked bar chart
    plt.figure(figsize=(16, 10))
    ax = plt.gca()

    # Add descriptive query labels
    df['query_label'] = df['query'].apply(get_query_label)

    # Group by query, layout, buffer_size, threads
    grouped = df.groupby(['query', 'layout', 'buffer_size', 'threads'])

    bar_positions = range(len(grouped))
    bar_labels = []

    # Create a mapping of pipeline IDs to their consistent colors
    pipeline_id_mapping = {}
    for col in pipeline_cols:
        pipeline_id = col.split('_')[1]
        pipeline_id_mapping[col] = PIPELINE_COLORS.get(pipeline_id, f'C{len(pipeline_id_mapping) % 10}')

    for i, ((query, layout, buffer_size, threads), group) in enumerate(grouped):
        # For each configuration, create a stacked bar
        config_label = f"{get_query_label(query)}\n{layout}\n{buffer_size}MB, {threads}T"
        bar_labels.append(config_label)

        # Extract pipeline percentages
        bottom = 0
        for col in sorted(pipeline_cols, key=lambda x: int(x.split('_')[1])):
            pipeline_id = col.split('_')[1]
            value = group[col].mean()

            # Use consistent color for the same pipeline ID
            color = pipeline_id_mapping[col]

            ax.bar(i, value, bottom=bottom, color=color,
                   label=f"Pipeline {pipeline_id}" if i == 0 else "")

            # Add text label inside the bar if there's enough space
            if value > 5:
                ax.text(i, bottom + value/2, f"P{pipeline_id}\n{value:.1f}%",
                        ha='center', va='center', color='white', fontweight='bold')

            bottom += value

    plt.title('Percentage of Time Spent in Each Pipeline by Configuration')
    plt.xlabel('Configuration')
    plt.ylabel('Percentage of Time (%)')
    plt.xticks(bar_positions, bar_labels, rotation=45, ha='right')
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys(), title='Pipeline')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'pipeline_percentages.png'))
    plt.close()

def create_throughput_by_parameter_chart(df, output_dir):
    """Create charts showing the relationship between parameters and throughput."""
    # Add descriptive query labels
    df['query_label'] = df['query'].apply(get_query_label)

    # Get all pipeline effective throughput columns
    pipeline_tp_cols = [col for col in df.columns if 'pipeline_' in col and 'eff_tp' in col]

    # Create a figure for each query
    for query in df['query'].unique():
        query_df = df[df['query'] == query]
        query_label = get_query_label(query)

        # Layout vs throughput (duration)
        plt.figure(figsize=(12, 8))
        ax = plt.gca()
        sns.barplot(x='layout', y='full_query_duration', data=query_df, ax=ax)
        data, unit = adjust_y_scale(ax, query_df['full_query_duration'])
        query_df['scaled_duration'] = data

        # Redraw with scaled data
        ax.clear()
        bars = sns.barplot(x='layout', y='scaled_duration', data=query_df, ax=ax)

        # Add value labels on top of bars
        for i, bar in enumerate(bars.patches):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.01,
                f"{query_df['full_query_duration'].iloc[i]:.4f}s",
                ha='center', va='bottom', rotation=0, size=10
            )

        plt.title(f'{query_label}: Duration by Memory Layout')
        plt.xlabel('Memory Layout')
        plt.ylabel(f'Duration ({unit})')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'query{query}_layout_duration.png'))
        plt.close()

        # COMBINED CHART: All pipelines with layouts in different colors
        if pipeline_tp_cols and len(query_df['layout'].unique()) > 1:
            # Filter to relevant pipelines for this query
            relevant_pipeline_cols = []
            for col in pipeline_tp_cols:
                # Check if this pipeline has data for this query
                if query_df[col].notna().any() and query_df[col].max() > 0:
                    relevant_pipeline_cols.append(col)

            if relevant_pipeline_cols:
                plt.figure(figsize=(14, 8))
                ax = plt.gca()

                # Define distinct colors for each layout
                layout_colors = {
                    'ROW_LAYOUT': '#1f77b4',       # blue
                    'COLUMNAR_LAYOUT': '#ff7f0e',  # orange
                }

                # Prepare data for line/marker plot
                pipeline_ids = []
                for col in relevant_pipeline_cols:
                    pipeline_ids.append(int(col.split('_')[1]))
                pipeline_ids = sorted(pipeline_ids)

                # Plot lines for each layout
                for layout in query_df['layout'].unique():
                    layout_rows = query_df[query_df['layout'] == layout]
                    if layout_rows.empty:
                        continue

                    throughputs = []
                    for pid in pipeline_ids:
                        col = f'pipeline_{pid}_eff_tp'
                        if col in layout_rows.columns:
                            throughputs.append(layout_rows[col].mean())
                        else:
                            throughputs.append(np.nan)

                    color = layout_colors.get(layout, 'gray')
                    ax.plot(pipeline_ids, throughputs, 'o-',
                            label=layout, color=color, linewidth=2, markersize=8)

                    # Add value labels for each point
                    for i, tp in enumerate(throughputs):
                        if not np.isnan(tp):
                            ax.annotate(f'{tp:.2e}',
                                        (pipeline_ids[i], tp),
                                        textcoords="offset points",
                                        xytext=(0,10),
                                        ha='center',
                                        fontsize=8)

                # Add grid for easier reading
                ax.grid(True, linestyle='--', alpha=0.7)

                # Set plot styling
                plt.title(f'{query_label}: Pipeline Throughput Comparison by Layout')
                plt.xlabel('Pipeline ID')
                plt.ylabel('Effective Throughput (tuples/s)')
                plt.yscale('log')  # Log scale for better visibility of differences
                plt.xticks(pipeline_ids)
                plt.legend()
                plt.tight_layout()
                plt.savefig(os.path.join(output_dir, f'query{query}_combined_pipeline_comparison.png'))
                plt.close()

                # Also create a bar chart version which may be easier to read
                plt.figure(figsize=(14, 8))
                ax = plt.gca()

                # Set width for the bars
                width = 0.35

                # Plot bars for each layout
                for i, layout in enumerate(query_df['layout'].unique()):
                    layout_rows = query_df[query_df['layout'] == layout]
                    if layout_rows.empty:
                        continue

                    throughputs = []
                    for pid in pipeline_ids:
                        col = f'pipeline_{pid}_eff_tp'
                        if col in layout_rows.columns:
                            throughputs.append(layout_rows[col].mean())
                        else:
                            throughputs.append(np.nan)

                    # Calculate bar positions
                    positions = np.array(pipeline_ids) + (i - 0.5*(len(query_df['layout'].unique())-1)) * width

                    color = layout_colors.get(layout, 'gray')
                    bars = ax.bar(positions, throughputs, width=width,
                                  label=layout, color=color, alpha=0.8)

                    # Add value labels on top of bars
                    for bar, tp in zip(bars, throughputs):
                        if not np.isnan(tp):
                            ax.text(
                                bar.get_x() + bar.get_width() / 2,
                                bar.get_height() * 1.05,
                                f"{tp:.2e}",
                                ha='center', va='bottom', rotation=90, size=8
                            )

                # Add grid for easier reading
                ax.grid(True, linestyle='--', alpha=0.7, axis='y')

                # Set plot styling
                plt.title(f'{query_label}: Pipeline Throughput Comparison by Layout')
                plt.xlabel('Pipeline ID')
                plt.ylabel('Effective Throughput (tuples/s)')
                plt.yscale('log')  # Log scale for better visibility of differences
                plt.xticks(pipeline_ids)
                plt.legend()
                plt.tight_layout()
                plt.savefig(os.path.join(output_dir, f'query{query}_combined_pipeline_bar_comparison.png'))
                plt.close()

        # Rest of the code remains the same...
        # (Individual pipeline charts, buffer size and threads charts)

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

    print(f"Charts created in directory: {output_dir}")

if __name__ == "__main__":
    main()