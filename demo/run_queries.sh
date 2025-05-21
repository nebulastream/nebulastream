#!/bin/bash

# Default values
TOPOLOGY_FILE="topology.yml"
QUERIES_FILE="queries.yaml"
GROUP_NAME=""
DELAY=1
ALL_QUERIES=false
DRY_RUN=false
VERBOSE=false
DUMP_DIR=""

# Function to display usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Execute nebuli commands for queries in a YAML file."
    echo
    echo "Options:"
    echo "  -t, --topology FILE    Topology file (default: topology.yml)"
    echo "  -q, --queries FILE     Queries file (default: queries.yaml)"
    echo "  -g, --group NAME       Process queries from this group"
    echo "  -a, --all              Process all queries regardless of group"
    echo "  --delay SECONDS        Delay between commands in seconds (default: 1)"
    echo "  -d, --dump DIR         Dump queries to DIR instead of registering them"
    echo "  --dry-run              Print commands without executing them"
    echo "  -v, --verbose          Show more details during execution"
    echo "  -h, --help             Display this help message"
    echo
    echo "Example:"
    echo "  $0 -t my_topology.yml -q my_queries.yaml -g benchmark"
    echo "  $0 -t my_topology.yml -q my_queries.yaml -g benchmark -d /tmp/queries"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--topology)
            TOPOLOGY_FILE="$2"
            shift 2
            ;;
        -q|--queries)
            QUERIES_FILE="$2"
            shift 2
            ;;
        -g|--group)
            GROUP_NAME="$2"
            shift 2
            ;;
        -a|--all)
            ALL_QUERIES=true
            shift
            ;;
        --delay)
            DELAY="$2"
            shift 2
            ;;
        -d|--dump)
            DUMP_DIR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check if required files exist
if [ ! -f "$TOPOLOGY_FILE" ]; then
    echo "Error: Topology file '$TOPOLOGY_FILE' not found"
    exit 1
fi

if [ ! -f "$QUERIES_FILE" ]; then
    echo "Error: Queries file '$QUERIES_FILE' not found"
    exit 1
fi

# Check if group is specified when needed
if [ "$ALL_QUERIES" = false ] && [ -z "$GROUP_NAME" ]; then
    echo "Error: Either specify a group with -g or use -a for all queries"
    usage
fi

# Check if python is available (for parsing YAML)
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required but not installed"
    exit 1
fi

# Function to extract queries using Python
extract_queries() {
    local all_flag="$1"
    local group_name="$2"
    local queries_file="$3"

    python3 -c "
import sys, yaml, json
with open('$queries_file', 'r') as f:
    data = yaml.safe_load(f)
if $all_flag:
    queries = [item['query'] for item in data.get('all_queries', [])]
    print(json.dumps(queries))
else:
    group = '$group_name'
    if group in data.get('queries_by_group', {}):
        queries = [item['query'] for item in data['queries_by_group'][group]]
        print(json.dumps(queries))
    else:
        sys.stderr.write(f'Error: Group {group} not found in {queries_file}\\n')
        sys.stderr.write('Available groups: ' + ', '.join(data.get('queries_by_group', {}).keys()) + '\\n')
        sys.exit(1)
"
}

# Function to execute a query
execute_query() {
    local query="$1"
    local index="$2"
    local command

    if [ -n "$DUMP_DIR" ]; then
        # Create dump directory if it doesn't exist
        if [ ! -d "$DUMP_DIR" ] && [ "$DRY_RUN" = false ]; then
            mkdir -p "$DUMP_DIR"
        fi

        # Generate a filename based on index and group
        local file_prefix
        if [ -n "$GROUP_NAME" ]; then
            file_prefix="${GROUP_NAME}_query_${index}"
        else
            file_prefix="query_${index}"
        fi
        echo "Dumping Plans for: $query"
        command="nebuli -t \"$TOPOLOGY_FILE\" dump -o \"$DUMP_DIR\" \"$query\""
    else
        command="nebuli -t \"$TOPOLOGY_FILE\" register -x \"$query\""
    fi

    if [ "$VERBOSE" = true ]; then
        echo "------------------------------"
        echo "Processing query #$index:"
        echo "$query"
        echo "Command:"
        echo "$command"
    else
        if [ -n "$DUMP_DIR" ]; then
            echo "Dumping query #$index..."
        else
            echo "Registering query #$index..."
        fi
    fi

    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] $command"
    else
        # Use eval to properly handle the quotes in the command
        eval "$command"

        # Check if the command succeeded
        if [ $? -eq 0 ]; then
            echo "Success!"
        else
            echo "Failed with exit code $?"
        fi
    fi

    if [ "$DELAY" -gt 0 ]; then
        sleep "$DELAY"
    fi
}

# Process queries
action_verb="Registering"
if [ -n "$DUMP_DIR" ]; then
    action_verb="Dumping"
fi

# Extract queries using Python
if [ "$ALL_QUERIES" = true ]; then
    echo "$action_verb all queries from '$QUERIES_FILE'"
    queries_json=$(extract_queries "True" "" "$QUERIES_FILE")

    # Check if extraction was successful
    if [ $? -ne 0 ]; then
        echo "Error extracting queries"
        exit 1
    fi

    # Parse JSON output to get queries
    queries=()
    while IFS= read -r query; do
        queries+=("$query")
    done < <(echo "$queries_json" | python3 -c "import json, sys; [print(q) for q in json.loads(sys.stdin.read())]")

    echo "Found ${#queries[@]} queries"

    # Process queries
    for i in "${!queries[@]}"; do
        index=$((i+1))
        execute_query "${queries[$i]}" "$index"
    done
else
    echo "$action_verb queries from group '$GROUP_NAME' in '$QUERIES_FILE'"
    queries_json=$(extract_queries "False" "$GROUP_NAME" "$QUERIES_FILE")

    # Check if extraction was successful
    if [ $? -ne 0 ]; then
        # Error message already printed by the Python script
        exit 1
    fi

    # Parse JSON output to get queries
    queries=()
    while IFS= read -r query; do
        queries+=("$query")
    done < <(echo "$queries_json" | python3 -c "import json, sys; [print(q) for q in json.loads(sys.stdin.read())]")

    echo "Found ${#queries[@]} queries in group '$GROUP_NAME'"

    # Process queries
    for i in "${!queries[@]}"; do
        index=$((i+1))
        execute_query "${queries[$i]}" "$index"
    done
fi

if [ -n "$DUMP_DIR" ] && [ "$DRY_RUN" = false ]; then
    echo "All queries processed successfully! Dumped to '$DUMP_DIR'"
else
    echo "All queries processed successfully!"
fi
