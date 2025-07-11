import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import sys
import os
from pathlib import Path

def analyze_sequence_shredder_results(csv_file_path, output_name=None):
    """
    Load CSV data into DuckDB, query it, and create visualization

    Args:
        csv_file_path (str): Path to the CSV file
        output_name (str): Optional output name for figures
    """

    # Connect to DuckDB (creates an in-memory database)
    conn = duckdb.connect()

    try:
        # Create the table
        create_table_sql = """
                           CREATE TABLE sequenceShredderResults (
                                                                    Dataset VARCHAR,
                                                                    Format VARCHAR,
                                                                    SequenceShredderImpl VARCHAR,
                                                                    NumberOfIterations INT64,
                                                                    NumberOfThreads INT64,
                                                                    RawBufferSize INT64,
                                                                    FormattedBufferSize INT64,
                                                                    MedianProcessingTime DOUBLE,
                                                                    AvgProcessingTime DOUBLE,
                                                                    MinProcessingTime DOUBLE,
                                                                    MaxProcessingTime DOUBLE
                           ); \
                           """
        conn.execute(create_table_sql)
        print("✓ Table created successfully")

        # Load data from CSV
        # DuckDB can handle CSV files with headers automatically
        copy_sql = f"COPY sequenceShredderResults FROM '{csv_file_path}' (HEADER, DELIMITER '|');"
        conn.execute(copy_sql)
        print("✓ Data loaded successfully")

        # Query data sorted by NumberOfThreads
        query_sql = """
                    SELECT
                        Dataset,
                        Format,
                        SequenceShredderImpl,
                        NumberOfThreads,
                        MedianProcessingTime,
                        AvgProcessingTime,
                        MinProcessingTime,
                        MaxProcessingTime
                    FROM sequenceShredderResults
                    ORDER BY NumberOfThreads ASC; \
                    """

        # Execute query and get results as pandas DataFrame
        df = conn.execute(query_sql).fetchdf()
        print(f"✓ Query executed successfully. Found {len(df)} rows")

        # Display first few rows
        print("\nFirst 5 rows of results:")
        print(df.head())

        # Create visualization
        figures_dir = create_output_directory()
        output_filename = get_output_filename(csv_file_path, output_name)
        create_visualization(df, figures_dir, output_filename)

        return df

    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        conn.close()

def create_visualization(df, figures_dir, output_filename):
    """
    Create visualization with NumberOfThreads on X-axis and MedianProcessingTime on Y-axis

    Args:
        df (pandas.DataFrame): Query results
        figures_dir (Path): Directory to save figures
        output_filename (str): Base filename for output files
    """

    # Set up the plot style
    plt.style.use('default')
    sns.set_palette("husl")

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 8))

    # Group by implementation type for different colored lines/points
    for impl in df['SequenceShredderImpl'].unique():
        impl_data = df[df['SequenceShredderImpl'] == impl]

        times = impl_data['MedianProcessingTime']
        threads = impl_data['NumberOfThreads']

        # Create the main scaling plot
        # fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        fig, (ax1) = plt.subplots(1, 1, figsize=(15, 6))

        # Plot 1: Execution Time vs Threads
        ax1.plot(threads, times, 'bo-', linewidth=2, markersize=8, label='Actual Performance')
        ax1.set_xlabel('Number of Threads')
        ax1.set_ylabel('Execution Time (seconds)')
        ax1.set_title('Execution Time vs Number of Threads')
        ax1.grid(True, alpha=0.3)
        ax1.legend()

        # Optional: Add ideal scaling line (perfect speedup)
        if len(threads) > 0 and len(times) > 0:
            ideal_times = [times[0] / t for t in threads]
            ax1.plot(threads, ideal_times, 'r--', alpha=0.7, label='Ideal Linear Scaling')
            ax1.legend()

        # Plot 2: Speedup vs Threads
        # if len(threads) > 0 and len(times) > 0:
        #     baseline_time = times[0]  # Single thread time as baseline
        #     speedup = [baseline_time / t for t in times]
        #
        #     ax2.plot(threads, speedup, 'go-', linewidth=2, markersize=8, label='Actual Speedup')
        #     ax2.plot(threads, threads, 'r--', alpha=0.7, label='Perfect Speedup')
        #     ax2.set_xlabel('Number of Threads')
        #     ax2.set_ylabel('Speedup Factor')
        #     ax2.set_title('Speedup vs Number of Threads')
        #     ax2.grid(True, alpha=0.3)
        #     ax2.legend()

        # Tight layout to prevent label cutoff
        plt.tight_layout()

        # Save the main plot
        main_plot_path = figures_dir / f"{output_filename}_main.png"
        plt.savefig(main_plot_path, dpi=300, bbox_inches='tight')
        print(f"✓ Main plot saved to: {main_plot_path}")

        # Close the figure to free memory
        plt.close(fig)

    # Customize the plot
    # ax.set_xlabel('Number of Threads', fontsize=12, fontweight='bold')
    # ax.set_ylabel('Median Processing Time (ms)', fontsize=12, fontweight='bold')
    # ax.set_title('Performance Analysis: Processing Time vs Number of Threads',
    #              fontsize=14, fontweight='bold', pad=20)
    #
    # # Add grid for better readability
    # ax.grid(True, alpha=0.3)
    #
    # # Set logarithmic scale for Y-axis if there's a wide range of values
    # if df['MedianProcessingTime'].max() / df['MedianProcessingTime'].min() > 10:
    #     # ax.set_yscale('log')
    #     ax.set_ylabel('Median Processing Time (ms) - Log Scale', fontsize=12, fontweight='bold')
    #
    # # Add legend
    # ax.legend(title='Implementation', loc='best', frameon=True, fancybox=True, shadow=True)

    # # Tight layout to prevent label cutoff
    # plt.tight_layout()
    #
    # # Save the main plot
    # main_plot_path = figures_dir / f"{output_filename}_main.png"
    # plt.savefig(main_plot_path, dpi=300, bbox_inches='tight')
    # print(f"✓ Main plot saved to: {main_plot_path}")
    #
    # # Close the figure to free memory
    # plt.close(fig)

def parse_arguments():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Analyze sequence shredder performance results from CSV file',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python script.py data.csv
  python script.py data.csv results_plot
  python script.py /path/to/YSB10K-CSV.csv performance_analysis
  python script.py results.csv --help
        '''
    )

    parser.add_argument(
        'csv_file_path',
        help='Path to the CSV file containing sequence shredder results'
    )

    parser.add_argument(
        'output_name',
        nargs='?',
        default=None,
        help='Name for the output figure files (without extension). If not specified, uses CSV filename.'
    )

    return parser.parse_args()

def create_output_directory():
    """
    Create the 'figures' directory if it doesn't exist
    """
    figures_dir = Path('figures')
    figures_dir.mkdir(exist_ok=True)
    return figures_dir

def get_output_filename(csv_file_path, output_name=None):
    """
    Generate output filename based on CSV file path or provided name

    Args:
        csv_file_path (str): Path to the CSV file
        output_name (str): Optional output name

    Returns:
        str: Base filename for output files
    """
    if output_name:
        return output_name
    else:
        # Extract filename without extension from CSV path
        return Path(csv_file_path).stem

def main():
    """
    Main function to run the analysis
    """
    # Parse command line arguments
    args = parse_arguments()
    csv_file_path = args.csv_file_path
    output_name = args.output_name

    # Check if file exists
    if not Path(csv_file_path).exists():
        print(f"Error: CSV file not found at '{csv_file_path}'")
        print("Please provide a valid path to your CSV file.")
        sys.exit(1)

    # Run the analysis
    print(f"Starting DuckDB analysis of: {csv_file_path}")
    if output_name:
        print(f"Output figures will be saved as: {output_name}_*.png")
    else:
        print(f"Output figures will be saved as: {Path(csv_file_path).stem}_*.png")

    df = analyze_sequence_shredder_results(csv_file_path, output_name)

    if df is not None:
        print("\n✓ Analysis completed successfully!")
        print(f"Total records processed: {len(df)}")
        print(f"Unique implementations: {df['SequenceShredderImpl'].nunique()}")
        print(f"Thread counts tested: {sorted(df['NumberOfThreads'].unique())}")
        print(f"Figures saved in: ./figures/")
    else:
        print("\n✗ Analysis failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()