#!/usr/bin/env python3
import os
import numpy as np
import pandas as pd
import multiprocessing
from datetime import datetime
import time

def generate_chunk(args):
    """Generate a chunk of IoT sensor data using vectorized operations"""
    chunk_id, chunk_size, start_row, start_timestamp, num_sensors, num_locations = args

    # Create random generator with different seed for each chunk
    rng = np.random.default_rng(seed=chunk_id)

    # Generate data in vectorized form
    sensor_ids = rng.integers(1, num_sensors + 1, size=chunk_size, dtype=np.int32)
    location_ids = rng.integers(1, num_locations + 1, size=chunk_size, dtype=np.int32)

    # Generate timestamps with increments
    increments = np.arange(start_row, start_row + chunk_size)
    timestamps = start_timestamp + increments * 60  # 1 minute intervals

    # Generate temperature with daily patterns
    base_temps = 20 + 10 * np.sin(2 * np.pi * (increments % (24*60)) / (24*60))
    temperatures = np.round(base_temps + rng.normal(0, 1.5, size=chunk_size), 2)

    # Generate humidity inversely related to temperature
    humidities = np.round(100 - temperatures * 1.5 + rng.normal(0, 5, size=chunk_size), 2)
    humidities = np.clip(humidities, 10, 100)

    # Generate remaining columns using vectorized operations
    pressures = np.round(1013.25 + rng.normal(0, 10, size=chunk_size), 2)
    light_levels = rng.lognormal(5, 1.5, size=chunk_size).astype(np.int32)
    noise_levels = np.round(rng.uniform(30, 90, size=chunk_size), 2)

    # Air quality - discrete values with noise
    base_aq = rng.choice([50, 100, 150, 200, 300], size=chunk_size)
    air_qualities = (base_aq + rng.integers(-20, 21, size=chunk_size)).astype(np.int32)
    co2_levels = (400 + rng.lognormal(3, 0.5, size=chunk_size)).astype(np.int32)

    # Generate remaining columns
    power_consumptions = np.round(rng.uniform(0.5, 5.0, size=chunk_size), 3)
    battery_levels = np.round(np.clip(100 - (increments % 1000) * 0.1, 0, 100), 2)

    signal_base = rng.choice([-90, -80, -70, -60, -50], size=chunk_size)
    signal_strengths = (signal_base + rng.integers(-5, 6, size=chunk_size)).astype(np.int32)

    # Sparse columns - vectorized
    error_counts = np.where(rng.random(size=chunk_size) > 0.95,
                            rng.integers(1, 11, size=chunk_size), 0).astype(np.int32)
    maintenance_flags = (rng.random(size=chunk_size) > 0.99).astype(np.int32)
    calibration_offsets = np.where(rng.random(size=chunk_size) > 0.9,
                                   np.round(rng.uniform(-0.5, 0.5, size=chunk_size), 3), 0.0)

    # Aggregated values
    avg_daily_temps = np.round(base_temps + rng.normal(0, 0.5, size=chunk_size), 2)
    max_daily_temps = np.round(avg_daily_temps + rng.uniform(3, 8, size=chunk_size), 2)
    min_daily_temps = np.round(avg_daily_temps - rng.uniform(3, 8, size=chunk_size), 2)

    # Status codes - vectorized selection
    status_probs = rng.random(size=chunk_size)
    status_codes = np.select(
        [status_probs < 0.8, status_probs < 0.85, status_probs < 0.9,
         status_probs < 0.95, status_probs < 0.98],
        [200, 201, 202, 400, 404],
        default=500
    ).astype(np.int32)

    # Create DataFrame with all columns
    df = pd.DataFrame({
        'sensor_id': sensor_ids,
        'location_id': location_ids,
        'timestamp': timestamps,
        'temperature': temperatures,
        'humidity': humidities,
        'pressure': pressures,
        'light_level': light_levels,
        'noise_level': noise_levels,
        'air_quality': air_qualities,
        'co2_level': co2_levels,
        'power_consumption': power_consumptions,
        'battery_level': battery_levels,
        'signal_strength': signal_strengths,
        'error_count': error_counts,
        'maintenance_flag': maintenance_flags,
        'calibration_offset': calibration_offsets,
        'avg_daily_temp': avg_daily_temps,
        'max_daily_temp': max_daily_temps,
        'min_daily_temp': min_daily_temps,
        'status_code': status_codes
    })

    return df

def generate_iot_dataset(total_rows, output_file, num_sensors=1000, num_locations=100):
    """Generate IoT sensor dataset in parallel chunks"""
    print(f"Generating {total_rows:,} rows of IoT data to {output_file}")
    start_time = time.time()

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Get available CPU cores
    cpu_count = multiprocessing.cpu_count()

    # Calculate optimal chunk size based on CPU count and available memory
    mem_gb = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / (1024**3)
    rows_per_gb = 5_000_000  # Approximate number of rows that fit in 1GB
    mem_based_chunk = int(mem_gb * rows_per_gb / (cpu_count * 2))

    # Use reasonable chunk size (not too small, not too large)
    chunk_size = min(max(100_000, mem_based_chunk), total_rows // cpu_count or total_rows)
    num_chunks = (total_rows + chunk_size - 1) // chunk_size

    print(f"Using {num_chunks} chunks with {chunk_size:,} rows per chunk")
    print(f"Using {cpu_count} CPU cores")

    # Start timestamp (January 1, 2024)
    start_timestamp = int(datetime(2024, 1, 1).timestamp())

    # Prepare arguments for parallel processing
    chunk_args = [
        (i,
         min(chunk_size, total_rows - i * chunk_size),
         i * chunk_size,
         start_timestamp,
         num_sensors,
         num_locations)
        for i in range(num_chunks)
    ]

    # Create a pool with all available cores
    with multiprocessing.Pool(processes=cpu_count) as pool:
        results = pool.map(generate_chunk, chunk_args)

    # Combine all chunks
    print("Combining chunks and writing to CSV...")

    # Write to CSV in chunks to minimize memory usage
    first_chunk = True
    for chunk_df in results:
        mode = 'w' if first_chunk else 'a'
        chunk_df.to_csv(output_file, index=False, header=False, mode=mode)
        first_chunk = False
        # Free memory
        del chunk_df

    elapsed_time = time.time() - start_time
    print(f"Dataset generation completed in {elapsed_time:.2f} seconds")
    print(f"Average speed: {total_rows / elapsed_time:.2f} rows/second")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate IoT sensor datasets")
    parser.add_argument("--rows", type=int, default=10_000_000,
                        help="Number of rows to generate")
    parser.add_argument("--output-dir", type=str,
                        default="/home/user/CLionProjects/nebulastream-public/Testing/data/iot/",
                        help="Output directory")

    args = parser.parse_args()

    # Generate datasets with command line arguments
    path = args.output_dir
    rows = args.rows

    generate_iot_dataset(rows, f"{path}iot_sensors_{rows//1_000_000}m.csv")