#!/bin/bash

# Check if correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <input_csv_path> <num_lines> <output_file>"
    exit 1
fi

# Assign arguments to named variables
input_file="$1"
num_lines="$2"
output_file="$3"

# Check if input file exists
if [ ! -f "$input_file" ]; then
    echo "Error: Input file $input_file does not exist"
    exit 1
fi

# Read specified number of lines and write to output file
head -n "$num_lines" "$input_file" > "$output_file"