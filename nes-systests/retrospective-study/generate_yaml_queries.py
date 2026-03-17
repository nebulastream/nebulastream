#!/usr/bin/env python3
"""Generate YAML query files from a .test file.

Usage:
    python generate_yaml_queries.py <test_file> [--physical-source <config_file>] [--output-dir <dir>]

Example:
    python generate_yaml_queries.py basic_alerts.test
    python generate_yaml_queries.py basic_alerts.test --physical-source physical_source_config.yaml --output-dir yaml_query_rules
"""

import argparse
import re
import sys
from pathlib import Path


def parse_schema(create_line: str) -> tuple[str, list[dict[str, str]]]:
    """Parse a CREATE LOGICAL SOURCE line into (source_name, schema_fields)."""
    match = re.match(r"CREATE LOGICAL SOURCE\s+(\w+)\((.+)\);", create_line)
    if not match:
        raise ValueError(f"Cannot parse CREATE LOGICAL SOURCE: {create_line}")
    source_name = match.group(1)
    fields_str = match.group(2)
    schema = []
    for field in fields_str.split(","):
        parts = field.strip().split()
        if len(parts) == 2:
            schema.append({"name": parts[0], "type": parts[1]})
        else:
            raise ValueError(f"Cannot parse field: {field.strip()}")
    return source_name, schema


def parse_test_file(test_file: Path) -> tuple[str, list[dict[str, str]], list[tuple[int, str, str]]]:
    """Parse a .test file and return (original_source_name, schema, queries).

    Each query is a tuple of (number, comment_line, sql_query).
    """
    lines = test_file.read_text().splitlines()

    original_source_name = None
    schema = None
    queries = []

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Parse CREATE LOGICAL SOURCE
        if line.startswith("CREATE LOGICAL SOURCE"):
            original_source_name, schema = parse_schema(line)
            i += 1
            continue

        # Parse query blocks: comment line starting with # [NN], followed by SELECT
        match = re.match(r"^#\s*\[(\d+)\]\s*(.*)", line)
        if match:
            query_num = int(match.group(1))
            comment = line
            # Next non-empty line should be the SELECT
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            if i < len(lines):
                sql_lines = []
                while i < len(lines) and lines[i].strip() and not lines[i].strip().startswith("----"):
                    sql_lines.append(lines[i].strip())
                    i += 1
                sql = "\n".join(sql_lines)
                queries.append((query_num, comment, sql))
            continue

        i += 1

    if original_source_name is None or schema is None:
        raise ValueError("No CREATE LOGICAL SOURCE found in test file")

    return original_source_name, schema, queries


def render_yaml(
    query_num: int,
    comment: str,
    sql: str,
    original_source_name: str,
    schema  : list[dict[str, str]],
    physical_source_text: str,
) -> str:
    """Render the YAML file content for a single query."""
    unique_source_name = f"{original_source_name}_{query_num:02d}"

    # Replace source name in SQL, strip trailing semicolons, and replace File() sink
    # Convert testing function to production function
    modified_sql = sql.replace("UnixTimestampToDatetimeTesting", "UnixTimestampToDatetime")
    modified_sql = modified_sql.replace(f"FROM {original_source_name}", f"FROM {unique_source_name}")
    modified_sql = modified_sql.rstrip().rstrip(";")
    modified_sql = modified_sql.replace(
        "INTO File()",
        f"INTO File('results/output_{query_num:02d}.csv' AS `SINK`.FILE_PATH,'CSV' as `SINK`.INPUT_FORMAT)",
    )

    # Build query section with literal block style
    sql_lines = modified_sql.split("\n")
    query_block = "query: |\n" + "\n".join(f"  {line}" for line in sql_lines)

    # Build logical section
    logical_lines = ["logical:"]
    logical_lines.append(f"  - name: {unique_source_name}")
    logical_lines.append("    schema:")
    for field in schema:
        logical_lines.append(f"      - name: {field['name']}")
        logical_lines.append(f"        type: {field['type']}")

    # Build physical section: inject unique source name, keep rest from config file
    # The physical_source_text is the raw YAML content of the config file
    physical_lines = ["physical:"]
    physical_lines.append(f"  - logical: {unique_source_name}")
    for line in physical_source_text.strip().splitlines():
        physical_lines.append(f"    {line}")

    parts = [
        f"#  {comment.lstrip('# ')}",
        query_block,
        "\n".join(logical_lines),
        "\n".join(physical_lines),
        "sinks: []",
    ]
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate YAML query files from a .test file")
    parser.add_argument("test_file", help="Path to the .test file")
    parser.add_argument(
        "--physical-source",
        default=None,
        help="Path to the physical source config YAML (default: physical_source_config.yaml next to this script)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for YAML files (default: yaml_query_rules/ next to the test file)",
    )
    args = parser.parse_args()

    test_file = Path(args.test_file)
    if not test_file.exists():
        print(f"Error: test file not found: {test_file}", file=sys.stderr)
        sys.exit(1)

    # Default physical source config: same directory as this script
    if args.physical_source:
        physical_source_file = Path(args.physical_source)
    else:
        physical_source_file = Path(__file__).parent / "physical_source_config.yaml"

    if not physical_source_file.exists():
        print(f"Error: physical source config not found: {physical_source_file}", file=sys.stderr)
        sys.exit(1)

    physical_source_text = physical_source_file.read_text()

    # Default output dir: yaml_query_rules/ next to the test file
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = test_file.parent / "yaml_query_rules"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse
    original_source_name, schema, queries = parse_test_file(test_file)
    test_name = test_file.stem  # e.g. "basic_alerts"

    # Generate
    for query_num, comment, sql in queries:
        yaml_content = render_yaml(
            query_num, comment, sql, original_source_name, schema, physical_source_text
        )

        filename = f"{query_num:02d}_{test_name}_rule.yaml"
        output_path = output_dir / filename
        output_path.write_text(yaml_content)

    print(f"Generated {len(queries)} YAML files in {output_dir}/")


if __name__ == "__main__":
    main()
