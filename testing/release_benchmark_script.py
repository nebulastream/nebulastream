#!/usr/bin/env python3

import subprocess
import sys
import tempfile
import os

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 script.py <numQueries>")
        sys.exit(1)

    num_queries = int(sys.argv[1])
    yaml_file_path = "/home/rudi/dima/nebulastream-public/testing/tcp_query.yaml"

    # Read the original YAML file
    with open(yaml_file_path, 'r') as f:
        original_yaml = f.read()

    # Register and start queries
    for i in range(1, num_queries + 1):
        # Create modified YAML with numbered output file
        modified_yaml = original_yaml.replace(
            'filePath: "output/outputQuery.csv"',
            f'filePath: "output/outputQuery{i}.csv"'
        )

        # Create temporary file with modified YAML
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            temp_file.write(modified_yaml)
            temp_yaml_path = temp_file.name

        try:
            # Register query with modified YAML
            register_cmd = f"/home/rudi/dima/nebulastream-public/cmake-build-release-docker-nes-development-local-libstd/nes-nebuli/nes-nebuli -d -w -s localhost:8080 register < {temp_yaml_path}"
            subprocess.run(register_cmd, shell=True)

            # Start query
            start_cmd = f"/home/rudi/dima/nebulastream-public/cmake-build-release-docker-nes-development-local-libstd/nes-nebuli/nes-nebuli -d -w -s localhost:8080 start {i}"
            subprocess.run(start_cmd, shell=True)
        finally:
            # Clean up temporary file
            os.unlink(temp_yaml_path)

if __name__ == "__main__":
    main()