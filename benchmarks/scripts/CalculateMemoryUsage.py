#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pandas as pd
import matplotlib.pyplot as plt


# Assume every buffer has the same size
buffer_size = 1
num_pooled_buffers = 200000

# Load data from CSV files
df1 = pd.read_csv('measurements/data/baseline.csv', header=None,
                  names=['numAvailablePooledBuf', 'numUnpooledBuf', 'timestamp'])
df2 = pd.read_csv('measurements/data/variant.csv', header=None,
                  names=['numAvailablePooledBuf', 'numUnpooledBuf', 'timestamp'])

# Calculate total memory consumption
df1['numUsedPooledBuf'] = num_pooled_buffers - df1['numAvailablePooledBuf']
df1['total_memory'] = (df1['numUsedPooledBuf'] + df1['numUnpooledBuf']) * buffer_size

df2['numUsedPooledBuf'] = num_pooled_buffers - df2['numAvailablePooledBuf']
df2['total_memory'] = (df2['numUsedPooledBuf'] + df2['numUnpooledBuf']) * buffer_size

# Convert timestamp to datetime
df1['datetime'] = pd.to_datetime(df1['timestamp'], unit='us')
df2['datetime'] = pd.to_datetime(df2['timestamp'], unit='us')

# Downsample the data by taking the average over a time window (e.g., every 100 data points)
window_size = 30  # Adjust this value based on your data density
df1_downsampled = df1.set_index('datetime').resample(f'{window_size}ms').mean().reset_index()
df2_downsampled = df2.set_index('datetime').resample(f'{window_size}ms').mean().reset_index()

# Normalize timestamps to span the full width of the plot
df1_downsampled['normalized_time'] = (df1_downsampled['datetime'] - df1_downsampled['datetime'].min()) / (
        df1_downsampled['datetime'].max() - df1_downsampled['datetime'].min())
df2_downsampled['normalized_time'] = (df2_downsampled['datetime'] - df2_downsampled['datetime'].min()) / (
        df2_downsampled['datetime'].max() - df2_downsampled['datetime'].min())

# Plot
fig, ax1 = plt.subplots(figsize=(12, 6))

# Plot solution 1
ax1.plot(df1_downsampled['normalized_time'], df1_downsampled['total_memory'], marker='o', linestyle='-', color='b',
         label='Baseline')
ax1.set_xlabel('Normalized Time')
ax1.set_ylabel('Total Number of Buffers (Baseline)', color='b')
ax1.tick_params(axis='y', labelcolor='b')

# Create a second y-axis for solution 2
ax2 = ax1.twinx()
ax2.plot(df2_downsampled['normalized_time'], df2_downsampled['total_memory'], marker='s', linestyle='--', color='r',
         label='Variant')
ax2.set_ylabel('Total Number of Buffers (Variant)', color='r')
ax2.tick_params(axis='y', labelcolor='r')

# Title and legend
plt.title('Total Memory Consumption Over Time Comparison')
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

plt.grid(True)
plt.tight_layout()
plt.show()
