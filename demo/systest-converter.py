#!/usr/bin/env python3
import os
import re
import sys
import yaml
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Extract source instructions from test files')
    parser.add_argument('--test-files', nargs='+', help='Path to test files')
    parser.add_argument('--test-files-list', help='Path to a file containing a list of test files')
    parser.add_argument('--output-dir', required=True, help='Output directory for source files')
    parser.add_argument('--testdata-path', default='TESTDATA', help='Path to replace TESTDATA in file paths')
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

def get_test_name(file_path):
    """Get a clean test name from the file path."""
    base_name = os.path.basename(file_path)
    # Remove .test extension if present
    if base_name.endswith('.test'):
        base_name = base_name[:-5]
    # Replace non-alphanumeric characters with underscores
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', base_name)
    return clean_name

def extract_source_csv(line, test_name, testdata_path):
    """Extract SourceCSV information."""
    pattern = r'SourceCSV\s+(\w+)\s+(.*?)(?=\s+\w+/|$)'
    match = re.search(pattern, line)
    if not match:
        return None, None

    orig_name = match.group(1)
    prefixed_name = f"{test_name}_{orig_name}"
    rest_of_line = line[match.end(1):].strip()

    # Extract path (which is at the end)
    path_pattern = r'(\S+/\S+\.csv)'
    path_match = re.search(path_pattern, rest_of_line)
    if not path_match:
        return None, None

    path = path_match.group(1)
    if path.startswith('TESTDATA'):
        path = path.replace('TESTDATA', testdata_path)

    # Extract schema - everything between the name and the path
    schema_str = rest_of_line[:path_match.start()].strip()

    # Parse the schema
    schema = []
    schema_parts = re.findall(r'(\w+)\s+(\w+)', schema_str)
    for type_name, column_name in schema_parts:
        schema.append({
            'type': type_name,
            'name': column_name
        })

    source_info = {
        'name': prefixed_name,
        'original_name': orig_name,
        'path': path,
        'schema': schema
    }

    # Create modified line
    modified_line = line.replace(f"SourceCSV {orig_name}", f"SourceCSV {prefixed_name}")

    return source_info, modified_line

def extract_source(content, start_line_idx, test_name, testdata_path):
    """Extract Source information and data rows."""
    lines = content.split('\n')
    start_line = lines[start_line_idx]

    # Extract the source name and schema
    pattern = r'Source\s+(\w+)\s+(.*?)$'
    match = re.search(pattern, start_line)
    if not match:
        return None, start_line_idx, None

    orig_name = match.group(1)
    prefixed_name = f"{test_name}_{orig_name}"
    schema_str = match.group(2)

    # Parse the schema
    schema = []
    schema_parts = re.findall(r'(\w+)\s+(\w+)', schema_str)
    for type_name, column_name in schema_parts:
        schema.append({
            'type': type_name,
            'name': column_name
        })

    # Extract data rows
    data_rows = []
    current_idx = start_line_idx + 1
    while current_idx < len(lines):
        line = lines[current_idx].strip()
        if not line or line.startswith('#') or line.startswith('Source') or line.startswith('SINK') or line.startswith('SELECT'):
            break
        data_rows.append(line)
        current_idx += 1

    source_info = {
        'name': prefixed_name,
        'original_name': orig_name,
        'schema': schema,
        'data': data_rows
    }

    # Create modified line
    modified_line = start_line.replace(f"Source {orig_name}", f"Source {prefixed_name}")

    return source_info, current_idx - 1, modified_line

def process_test_file(file_path, output_dir, testdata_path):
    """Process a test file to extract all sources."""
    sources = []
    modified_content = []
    name_mapping = {}  # Map original source names to prefixed names

    test_name = get_test_name(file_path)

    with open(file_path, 'r') as file:
        content = file.read()

    lines = content.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if line.startswith('SourceCSV'):
            source_info, modified_line = extract_source_csv(line, test_name, testdata_path)
            if source_info:
                sources.append(source_info)
                name_mapping[source_info['original_name']] = source_info['name']
                modified_content.append(modified_line)
            else:
                modified_content.append(lines[i])

        elif line.startswith('Source'):
            source_info, last_idx, modified_line = extract_source(content, i, test_name, testdata_path)
            if source_info:
                # Create a data file for this source
                output_file = os.path.join(output_dir, f"{source_info['name']}.csv")
                with open(output_file, 'w') as out_file:
                    out_file.write('\n'.join(source_info['data']))

                # Update the source info with the path
                source_info['path'] = output_file
                del source_info['data']  # Remove the data from the output

                sources.append(source_info)
                name_mapping[source_info['original_name']] = source_info['name']
                modified_content.append(modified_line)

                # Add the data lines to modified content
                for j in range(i+1, last_idx+1):
                    modified_content.append(lines[j])

                i = last_idx  # Skip to the last line of data
            else:
                modified_content.append(lines[i])

        else:
            # Check if line contains any of the original source names and replace them
            current_line = lines[i]
            for orig_name, new_name in name_mapping.items():
                # Replace only whole words that match the source name
                current_line = re.sub(r'\b' + re.escape(orig_name) + r'\b', new_name, current_line)
            modified_content.append(current_line)

        i += 1

    # Create a modified test file with the prefixed source names
    test_basename = os.path.basename(file_path)
    modified_test_path = os.path.join(output_dir, f"modified_{test_basename}")
    with open(modified_test_path, 'w') as mod_file:
        mod_file.write('\n'.join(modified_content))

    return sources, modified_test_path, name_mapping

def main():
    args = parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    all_sources = []
    all_modified_paths = []
    all_name_mappings = {}

    for test_file in args.test_files:
        test_file = test_file.strip()
        if not test_file:
            continue

        print(f"Processing file: {test_file}")
        try:
            sources, modified_path, name_mapping = process_test_file(test_file, args.output_dir, args.testdata_path)
            all_sources.extend(sources)
            all_modified_paths.append(modified_path)
            all_name_mappings[test_file] = name_mapping
        except Exception as e:
            print(f"Error processing file {test_file}: {e}")

    # Output the sources in YAML format
    output = {'sources': all_sources}
    yaml_output = yaml.dump(output, sort_keys=False, default_flow_style=False)

    # Write the YAML to a file
    output_yaml = os.path.join(args.output_dir, 'sources.yaml')
    with open(output_yaml, 'w') as yaml_file:
        yaml_file.write(yaml_output)

    # Write the name mappings to a file
    mappings_yaml = os.path.join(args.output_dir, 'name_mappings.yaml')
    with open(mappings_yaml, 'w') as mapping_file:
        yaml.dump(all_name_mappings, mapping_file, sort_keys=False, default_flow_style=False)

    print(f"Extracted {len(all_sources)} sources to {output_yaml}")
    print(f"Created modified test files in {args.output_dir}")
    print(f"Name mappings saved to {mappings_yaml}")

if __name__ == '__main__':
    main()
