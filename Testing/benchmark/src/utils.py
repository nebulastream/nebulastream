import os
import subprocess
import argparse
import re
import shutil
from pathlib import Path
import json
import csv
import pandas as pd
import concurrent.futures
from collections import defaultdict
import multiprocessing
import numpy as np
import time
import glob

def get_config_from_file(config_file):
    # Read the query mapping file to get query configurations
    if config_file.suffix == '.txt':
        config={}
        with open(config_file, 'r') as f:
            for line in f:
                name, value = line.split(':')
                config[name.strip()] = value.strip()

        return config

    #else assume csv
    query_configs = []
    if config_file.exists():
        query_configs = pd.read_csv(config_file, index_col='query_id').to_dict(orient='index')
        #{'test_id': 1, 'buffer_size': 4000, 'num_columns': 1, 'accessed_columns': 1, 'operator_chain': "['filter']", 'swap_strategy': 'USE_SINGLE_LAYOUT', 'selectivity': 5.0, 'individual_selectivity': 5.0, 'threshold': 4080218930.0, 'function_type': nan, 'aggregation_function': nan, 'window_size': nan, 'num_groups': nan, 'id_data_type': nan}
        if config_file.suffix == '.csvo': # old version
            config=defaultdict(dict)
            columns= []
            for col in range(len(query_configs)):
                columns.append(query_configs[col]['query_id'])
            for query_id in range(len(query_configs[1])):
                for col in range(len(columns)):
                    config[query_id][col] = query_configs[col]['query_id']
            return config

    else:
        #raise error if file does not exist
        raise FileNotFoundError(f"Config file {config_file} does not exist.")


    return query_configs

def extract_metadata_from_filename(file_path):
    """Extract metadata (layout, buffer size, threads, query) from filename."""
    # Get just the filename from path
    filename = os.path.basename(file_path)

    # Fix duplicate filenames like "file.jsonfile.json"
    if filename.endswith(".csv"):
        duplicate_pos = filename.find(".csv", 0, -5)
        if duplicate_pos > 0:
            filename = filename[:duplicate_pos + 5]

    metadata = {}
    metadata['filename'] = file_path
    query_dir_path = Path(file_path).parent.parent.parent
    metadata['config'] = get_config_from_file(query_dir_path / "config.txt")
    query_id= re.search(r'_query(\d+).json', filename)
    if query_id:
        metadata['query_id'] = int(query_id.group(1))

    # Extract layout with more specific pattern
    if '_ROW_LAYOUT_' in filename:
        metadata['layout'] = 'ROW_LAYOUT'
    elif '_COLUMNAR_LAYOUT_' in filename:
        metadata['layout'] = 'COLUMNAR_LAYOUT'

    # Extract buffer size with more specific pattern
    buffer_match = re.search(r'_buffer(\d+)_', filename)
    if buffer_match:
        metadata['buffer_size'] = int(buffer_match.group(1))

    # Extract thread count with more specific pattern
    threads_match = re.search(r'_threads(\d+)_', filename)
    if threads_match:
        metadata['threads'] = int(threads_match.group(1))

    # Extract query number with more specific pattern
    query_match = re.search(r'_query(\d+)', filename)
    if query_match:
        metadata['query'] = int(query_match.group(1))

    if metadata == {}:
        print(f"Warning: No metadata extracted from filename: {filename}")
    return metadata

def parse_int_list(arg):
    """Convert comma-separated string to list of integers."""
    return [int(x) for x in arg.split(',')]

def parse_str_list(arg):
    """Convert comma-separated string to list of strings."""
    return [x.strip() for x in arg.split(',')]
