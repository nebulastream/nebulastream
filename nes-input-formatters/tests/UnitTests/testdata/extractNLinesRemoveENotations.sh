#!/bin/bash

if [ "$#" -ne 3 ]; then
   echo "Usage: $0 <input_csv_path> <num_lines> <output_file>"
   exit 1
fi

input_file="$1"
num_lines="$2"
output_file="$3"

if [ ! -f "$input_file" ]; then
   echo "Error: Input file $input_file does not exist"
   exit 1
fi

# Read lines, process e+, and write to output
head -n "$num_lines" "$input_file" | sed 's/e+[^|]*\([|]\|$\)/\1/g' > "$output_file"