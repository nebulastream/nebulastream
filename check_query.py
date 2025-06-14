import duckdb
import pandas as pd
import argparse
import sys
import csv
from pathlib import Path

# Create an argument parser
parser = argparse.ArgumentParser(description="Parse a file path from the command line.")

# Add a file path argument
parser.add_argument(
    'filepath',
    type=Path,
    help='Path to the input file'
)


parser.add_argument(
    '--stdout',
    action='store_true',
    help='Write output to stdout instead of a file'
)

# Parse the arguments
args = parser.parse_args()

# Access the file path
file_path = args.filepath

# Connect to DuckDB (in-memory database for this example)
conn = duckdb.connect()

# Create the schema for the table based on the new column types
conn.execute("""
CREATE TABLE ysb1k (
    user_id VARCHAR,
    page_id VARCHAR,
    campaign_id VARCHAR,
    ad_id VARCHAR,
    ad_type VARCHAR,
    event_type VARCHAR,
    event_time UINT64,
    ip_address VARCHAR
);
""")

# Read the CSV file directly into DuckDB using its built-in `read_csv` function
# Replace 'your_file.csv' with the actual path to your CSV file
conn.execute(f"COPY ysb1k FROM '{file_path.as_posix()}' (FORMAT CSV, HEADER FALSE);")

# Now the table is populated, let's perform the stream emulation query
# Convert the `event_time` from UINT64 to TIMESTAMP and use it in the query
query = """
SELECT 
    floor(extract(epoch FROM TIMESTAMP 'epoch' + CAST(floor(event_time / 1000) AS BIGINT) * INTERVAL '1 second') / $1) * $1 AS start,
    (floor(extract(epoch FROM TIMESTAMP 'epoch' + CAST(floor(event_time / 1000) AS BIGINT) * INTERVAL '1 second') / $1) + 1) * $1 AS end,
    campaign_id,
    COUNT(user_id) AS count_user_id
FROM ysb1k
WHERE event_type = 'view'
GROUP BY campaign_id, floor(extract(epoch FROM TIMESTAMP 'epoch' + CAST(floor(event_time / 1000) AS BIGINT) * INTERVAL '1 second') / $1)
ORDER BY start, campaign_id;
"""

# Set pandas to display full precision for floating-point numbers and avoid scientific notation
pd.set_option('display.float_format', lambda x: '%.10f' % x)

window_size_in_sec = 30
# Execute the query and get the results into a pandas DataFrame
result_df = conn.execute(query, [window_size_in_sec]).fetchdf()

# Ensure the `start` and `end` columns remain in milliseconds, as integers
result_df['start'] = result_df['start'].astype('int64')
result_df['end'] = result_df['end'].astype('int64')

result_df['start'] = result_df['start'] * 1000
result_df['end'] = result_df['end'] * 1000

if args.stdout:
    dst = sys.stdout
else:
    dst = 'check_output.csv'

# Output the results to a CSV file (acting as printSink1k)
result_df.to_csv(dst, index=False, header=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

# Close the connection
conn.close()

# Display the result (optional)
#print(result_df)

