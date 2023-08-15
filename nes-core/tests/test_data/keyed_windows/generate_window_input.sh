#!/bin/bash

# Script: generate_csv_data.sh
# Description: This Bash script generates multiple CSV files with customizable data rows.
# Each CSV file contains rows with IDs, values, and group numbers. The script takes
# command line arguments to determine the number of runs, starting ID, number of lines
# in each group, number of groups to write, starting group number, and output file prefix.
# For each run, it generates a CSV file with specified data rows.

# Check if the number of command line arguments is not equal to 6
if [ $# -ne 6 ]; then
  echo "Usage: $0 <num_runs> <id_start> <num_lines_in_group> <num_groups_to_write> <group_num_start> <output_file_prefix>"
  exit 1
fi

# Assign command line arguments to variables
num_runs=$1                  # Number of runs
id_start=$2                  # Starting ID
num_lines_in_group=$3        # Number of lines in each group
num_groups_to_write=$4       # Number of groups to write
group_num_start=$5           # Starting group number
output_file_prefix=$6        # Prefix for output file names

# Check if the output file for the starting ID already exists, and delete it if it does
if [ -e "${output_file_prefix}_${id_start}.csv" ]; then
  echo "Output file already exists, deleting: ${output_file_prefix}_${id_start}.csv"
  rm "${output_file_prefix}_${id_start}.csv"
fi

# Loop through the range of IDs to generate CSV files for each ID
for i in $(seq $id_start $(($id_start + $num_runs - 1))); do
  id=$i
  output_file_path="${output_file_prefix}_${id}.csv"

  # Check if the output file for the current ID already exists, and delete it if it does
  if [ -e "$output_file_path" ]; then
    echo "Output file already exists, deleting: $output_file_path"
    rm "$output_file_path"
  fi

  echo "Generating CSV file $output_file_path"
  # Uncomment the following line if you want to include a header in the CSV file
  # echo "ID,Value,Group" > "$output_file_path"

  # Generate CSV data for each group and write to the output file
  for j in $(seq 1 $num_groups_to_write); do
    for k in $(seq 1 $num_lines_in_group); do
      group_num=$(( $group_num_start + (($j - 1) * 1000) ))
      value=$(shuf -i 1-20 -n 1)

      echo "$id,$value,$group_num" >> "$output_file_path"
    done
  done
done
