import os
import sys
import glob
import re
import json
import pandas as pd

RUN_SEGMENT = re.compile(
    r"Running test with layout=(?P<layout>\w+)_LAYOUT and bufferSize=(?P<buffer>\d+)(?P<body>.*?)(?=(?:Running test with layout=|\Z))",
    re.DOTALL
)
SQL_BLOCK   = re.compile(r"={3,}\n(?P<sql>SELECT[\s\S]*?)\n={3,}", re.IGNORECASE)
JSON_BLOCK  = re.compile(r"\[\s*\{.*?\}\s*\]", re.DOTALL)

def parse_log(path):
    text = open(path).read()
    rows = []
    for seg in RUN_SEGMENT.finditer(text):
        layout       = seg.group("layout")
        buffer_bytes = int(seg.group("buffer"))
        body         = seg.group("body")

        sqls  = [m.group("sql").strip() for m in SQL_BLOCK.finditer(body)]
        jsons = JSON_BLOCK.findall(body)

        for sql, jb in zip(sqls, jsons):
            for rec in json.loads(jb):
                rows.append({
                    "query": sql,
                    "memory_layout": layout,
                    "buffer_bytes": buffer_bytes,
                    "pipeline_id": rec.get("pipeline_id", 0),
                    "time_s": rec["time"],
                    "throughput": rec.get("bytesPerSecond"),
                    "tuples_per_second": rec.get("tuplesPerSecond")
                })
    return rows

def main(target):
    files = [target] if os.path.isfile(target) \
        else glob.glob(os.path.join(target, "**", "*.log"), recursive=True)

    all_rows = []
    for f in files:
        all_rows.extend(parse_log(f))

    df = pd.DataFrame(all_rows)
    df["total_time_s"]   = df.groupby(["query","memory_layout","buffer_bytes"])["time_s"].transform("sum")
    df["pct_of_total"]   = df["time_s"] / df["total_time_s"] * 100

    out = os.path.join(os.getcwd(), "results.csv")
    df.to_csv(out, index=False)
    print(f"Written results to `results.csv`")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {os.path.basename(sys.argv[0])} path/to/logs")
        sys.exit(1)
    main(sys.argv[1])