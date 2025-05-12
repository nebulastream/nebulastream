#!/usr/bin/env python3
import re
import os
import yaml
import argparse
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser(description='Extract queries from test files and group by test group')
    parser.add_argument('--test-files', nargs='+', required=True, help='Path to test files')
    parser.add_argument('--test-files-list', help='Path to a file containing a list of test files')
    parser.add_argument('--output', default='queries.yaml', help='Output file path')
    parser.add_argument('--name-mappings', help='Path to name_mappings.yaml file from extract_sources.py')
    args = parser.parse_args()

    # Check if either test-files or test-files-list is provided
    if not args.test_files and not args.test_files_list:
        parser.error("Either --test-files or --test-files-list must be provided")

    # If test-files-list is provided, read the list of files
    if args.test_files_list:
        with open(args.test_files_list, 'r') as f:
            file_list = [line.strip() for line in f if line.strip()]
        if args.test_files:
            args.test_files.extend(file_list)
        else:
            args.test_files = file_list

    return args

def load_name_mappings(mappings_file):
    """Load source name mappings from a YAML file."""
    if not mappings_file:
        return {}

    try:
        with open(mappings_file, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load name mappings file {mappings_file}: {e}")
        return {}

def extract_queries_from_file(file_path, name_mappings=None):
    """Extract queries from a test file."""
    try:
        with open(file_path, 'r') as file:
            content = file.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None, []

    # Extract test name and groups from the header
    test_name = None
    test_groups = []

    # Extract from header comments
    header_pattern = r"^#\s*name:\s*(.+?)\s*$"
    name_match = re.search(header_pattern, content, re.MULTILINE)
    if name_match:
        test_name = name_match.group(1).strip()
    else:
        # Use filename as test name if not found in header
        test_name = os.path.basename(file_path)

    groups_pattern = r"^#\s*groups:\s*\[(.+?)\]"
    groups_match = re.search(groups_pattern, content, re.MULTILINE)
    if groups_match:
        groups_str = groups_match.group(1)
        test_groups = [g.strip() for g in groups_str.split(',')]

    # Split content into sections by '----'
    sections = content.split('\n----\n')
    if len(sections) == 1:  # Try alternative split
        sections = content.split('\n----')

    queries = []

    # Process each section before a '----' marker
    for i in range(len(sections) - 1):  # Skip the last section which is after the last '----'
        section = sections[i]

        # Find the position of the first empty line or comment line after any results
        lines = section.splitlines()

        # Skip empty sections
        if not lines:
            continue

        # Start by finding any numeric result lines (from previous query)
        result_end_idx = 0
        for j, line in enumerate(lines):
            if re.match(r'^\s*\d+', line.strip()):
                result_end_idx = j + 1

        # Find the first empty line or comment after the results
        query_start_idx = result_end_idx
        for j in range(result_end_idx, len(lines)):
            if not lines[j].strip() or lines[j].strip().startswith('#'):
                query_start_idx = j + 1
                break

        # If we didn't find an empty or comment line, just use where we left off
        # Extract the query part (after empty/comment line, before ----)
        if query_start_idx < len(lines):
            query_lines = lines[query_start_idx:]

            # Filter out any leading empty lines
            while query_lines and not query_lines[0].strip():
                query_lines.pop(0)

            # Filter out comment lines that might be within the query
            query_lines = [line for line in query_lines if not line.strip().startswith('#')]

            # Combine all lines into a single line, preserving spaces between words
            query = ' '.join([line.strip() for line in query_lines if line.strip()])

            if query:
                queries.append(query)

    # Try to extract queries from the last section too
    if sections:
        last_section = sections[-1]
        lines = last_section.splitlines()

        # Similar logic - find the first empty or comment line after any result lines
        result_end_idx = 0
        for j, line in enumerate(lines):
            if re.match(r'^\s*\d+', line.strip()):
                result_end_idx = j + 1

        # Find the first empty line or comment after the results
        query_start_idx = result_end_idx
        for j in range(result_end_idx, len(lines)):
            if not lines[j].strip() or lines[j].strip().startswith('#'):
                query_start_idx = j + 1
                break

        if query_start_idx < len(lines):
            query_lines = lines[query_start_idx:]

            # Filter out any leading empty lines
            while query_lines and not query_lines[0].strip():
                query_lines.pop(0)

            # Filter out comment lines
            query_lines = [line for line in query_lines if not line.strip().startswith('#')]

            # Combine all lines into a single line
            query = ' '.join([line.strip() for line in query_lines if line.strip()])

            if query and not query.startswith('Source') and ('SELECT' in query or 'INTO' in query):
                queries.append(query)

    # Replace source names in queries if name mappings are provided
    if name_mappings and file_path in name_mappings:
        file_mappings = name_mappings[file_path]

        updated_queries = []
        for query in queries:
            updated_query = query
            for orig_name, new_name in file_mappings.items():
                # Replace only whole words that match the source name
                updated_query = re.sub(r'\b' + re.escape(orig_name) + r'\b', new_name, updated_query)
            updated_queries.append(updated_query)

        queries = updated_queries

    return {
        "test_name": test_name,
        "groups": test_groups
    }, queries

def main():
    args = parse_args()

    # Load name mappings if provided
    name_mappings = load_name_mappings(args.name_mappings)

    # Dictionary to store queries grouped by test groups
    queries_by_group = defaultdict(list)
    all_queries = []

    # Process each test file
    for test_file in args.test_files:
        test_file = test_file.strip()
        if not test_file:
            continue

        print(f"Processing file: {test_file}")
        test_info, queries = extract_queries_from_file(test_file, name_mappings)

        if not test_info or not queries:
            print(f"  No queries found in {test_file}")
            continue

        test_name = test_info["test_name"]

        # Add queries to the group-specific and global lists
        for query in queries:
            # Skip empty queries or source definitions
            if not query.strip() or query.strip().startswith('Source'):
                continue

            query_info = {
                "test_name": test_name,
                "query": query
            }
            all_queries.append(query_info)

            # Add to each group - create a full copy of the query_info for each group
            for group in test_info["groups"]:
                # Create a new dict to avoid reference issues
                group_query_info = {
                    "test_name": test_name,
                    "query": query
                }
                queries_by_group[group].append(group_query_info)

        print(f"  Extracted {len(queries)} queries from {test_file}")

        # Print source name replacements if mappings were applied
        if name_mappings and test_file in name_mappings and name_mappings[test_file]:
            print(f"  Applied source name replacements in queries:")
            for orig, new in name_mappings[test_file].items():
                print(f"    {orig} → {new}")

    # Create the output structure
    output = {
        "all_queries": all_queries,
        "queries_by_group": dict(queries_by_group)
    }

    # Custom YAML dumper to ensure strings don't get wrapped
    class NoAliasDumper(yaml.SafeDumper):
        def ignore_aliases(self, data):
            return True

        def represent_scalar(self, tag, value, style=None):
            # Force double quotes for all strings to prevent wrapping
            if tag == 'tag:yaml.org,2002:str':
                style = '"'
            return super().represent_scalar(tag, value, style)

    # Write to YAML file with our custom dumper
    with open(args.output, 'w') as file:
        yaml.dump(output, file, default_flow_style=False, width=float("inf"), Dumper=NoAliasDumper)

    print(f"\nExtracted {len(all_queries)} queries to {args.output}")
    print(f"Queries grouped by {len(queries_by_group)} test groups")

    # Print a summary of groups and counts
    print("\nSummary of query groups:")
    for group, queries in sorted(queries_by_group.items()):
        print(f"  {group}: {len(queries)} queries")

if __name__ == "__main__":
    main()
