#!/bin/bash

if [ $# -ne 6 ]; then
  echo "Usage: $0 <num_runs> <id_start> <num_lines_in_group> <num_groups_to_write> <group_num_start> <output_file_prefix>"
  exit 1
fi

num_runs=$1
id_start=$2
num_lines_in_group=$3
num_groups_to_write=$4
group_num_start=$5
output_file_prefix=$6

if [ -e "${output_file_prefix}_${id_start}.csv" ]; then
  echo "Output file already exists, deleting: ${output_file_prefix}_${id_start}.csv"
  rm "${output_file_prefix}_${id_start}.csv"
fi

for i in $(seq $id_start $(($id_start + $num_runs - 1))); do
  id=$i
  output_file_path="${output_file_prefix}_${id}.csv"

  if [ -e "$output_file_path" ]; then
    echo "Output file already exists, deleting: $output_file_path"
    rm "$output_file_path"
  fi

  echo "Generating CSV file $output_file_path"
  #echo "ID,Value,Group" > "$output_file_path"

  for j in $(seq 1 $num_groups_to_write); do
    for k in $(seq 1 $num_lines_in_group); do
      group_num=$(( $group_num_start + (($j - 1) * 1000) ))
      value=$(shuf -i 1-20 -n 1)

      echo "$id,$value,$group_num" >> "$output_file_path"
    done
  done
done
