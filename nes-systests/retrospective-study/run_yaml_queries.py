#!/usr/bin/env python3
"""Run all YAML query files in a directory using nes-cli.

Usage:
    python run_yaml_queries.py <yaml_dir> [--cli <path_to_nes_cli>]

Example:
    python run_yaml_queries.py yaml_query_rules
    python run_yaml_queries.py yaml_query_rules --cli path/to/nes-cli
"""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Run all YAML query files in a directory")
    parser.add_argument("yaml_dir", help="Directory containing YAML query files")
    parser.add_argument(
        "--cli",
        default=None,
        help="Path to nes-cli binary (default: auto-detect /home/rudi/dima/nebulastream-public/cmake-build-debug-docker-nes-development-local-libstd/nes-frontend/apps/nes-cli relative to repo root)",
    )
    args = parser.parse_args()

    yaml_dir = Path(args.yaml_dir)
    if not yaml_dir.is_dir():
        print(f"Error: directory not found: {yaml_dir}", file=sys.stderr)
        sys.exit(1)

    if args.cli:
        cli_path = Path(args.cli)
    else:
        # Auto-detect: walk up from this script to find repo root (where cmake-build-* lives)
        repo_root = Path(__file__).resolve().parent
        while repo_root != repo_root.parent:
            candidate = repo_root / "cmake-build-debug-docker-nes-development-local-libstd" / "nes-frontend" / "apps" / "nes-cli"
            if candidate.exists():
                cli_path = candidate
                break
            repo_root = repo_root.parent
        else:
            print("Error: could not find nes-cli binary. Use --cli to specify the path.", file=sys.stderr)
            sys.exit(1)

    if not cli_path.exists():
        print(f"Error: nes-cli not found at: {cli_path}", file=sys.stderr)
        sys.exit(1)

    yaml_files = sorted(yaml_dir.glob("*.yaml"))
    if not yaml_files:
        print(f"No YAML files found in {yaml_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Running {len(yaml_files)} queries with {cli_path}")

    failed = []
    for yaml_file in yaml_files:
        print(f"\n--- {yaml_file.name} ---")
        result = subprocess.run([str(cli_path), "-t", str(yaml_file), "start"])
        if result.returncode != 0:
            failed.append(yaml_file.name)
            print(f"FAILED (exit code {result.returncode})")

    print(f"\n{'='*40}")
    print(f"Completed: {len(yaml_files) - len(failed)}/{len(yaml_files)} succeeded")
    if failed:
        print(f"Failed: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
