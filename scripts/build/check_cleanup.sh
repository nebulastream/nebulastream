#!/bin/bash

# Function to calculate the age of a file in days
file_age_in_days() {
    local file=$1
    local now=$(date +%s)
    local file_timestamp=$(date -r "$file" +%s)
    local age_seconds=$((now - file_timestamp))
    local age_days=$((age_seconds / 86400))
    echo $age_days
}

# Function to calculate the age of a file in minutes
file_age_in_minutes() {
    local file=$1
    local now=$(date +%s)
    local file_timestamp=$(date -r "$file" +%s)
    local age_seconds=$((now - file_timestamp))
    local age_minutes=$((age_seconds / 60))
    echo $age_minutes
}

# Parsing the parameters and printing them to stdout
file=$1
cleanupThresholdInDays=5
current_time=$(date +%s)

if [ -f "$file" ]; then
    age=$(file_age_in_days "$file")
    if [ $((age)) -gt cleanupThresholdInDays ]; then
        echo "$current_time" > "$file"
        echo 1
    else
        echo 0
    fi
else
    echo "$current_time" > "$file"
    echo 0
fi
