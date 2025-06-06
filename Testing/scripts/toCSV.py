import os, sys, glob, re, json
import pandas as pd

# 1) match only the invocation line and capture layout+buffer
HEADER_RE = re.compile(
    r'^(/tmp/.*--worker\.queryCompiler\.memoryLayoutType=(?P<layout>\w+)_LAYOUT.*?--worker\.bufferSizeInBytes=(?P<buffer>\d+).*)$',
    re.MULTILINE
)

SQL_BLOCK = re.compile(r'={3,}\s*\n(?P<sql>SELECT[\s\S]*?)\n={3,}', re.IGNORECASE)
JSON_BLOCK = re.compile(r'\[\s*\{[\s\S]*?\}\s*\]', re.DOTALL)
PIPELINE_RE = re.compile(
    r'Pipeline\s+(?P<pid>\d+):\s+(?P<tuples>[\d,]+)\s+tuples,\s+(?P<tupsps>[\d\.]+)\s+tuples/s'
)

def parse_log(path):
    text = open(path).read()
    rows = []
    headers = list(HEADER_RE.finditer(text))
    for i, hm in enumerate(headers):
        layout       = hm.group('layout')
        buffer_bytes = int(hm.group('buffer'))
        # slice body from end of this header to start of next (or EOF)
        start = hm.end()
        end   = headers[i+1].start() if i+1 < len(headers) else len(text)
        body  = text[start:end]
        print(body)
        sql_m  = SQL_BLOCK.search(body)
        json_m = JSON_BLOCK.search(body)
        if not sql_m or not json_m:
            continue

        sql = sql_m.group('sql').strip()
        rec = json.loads(json_m.group(0))[0]
        print(rec)
        print(sql)
        common = {
            "query":            sql,
            "memory_layout":    layout,
            "buffer_bytes":     buffer_bytes,
            "time_s":           rec["time"],
            "bytes_per_second": rec.get("bytesPerSecond")
        }

        for m in PIPELINE_RE.finditer(body):
            rows.append({
                **common,
                "pipeline_id":       int(m.group("pid")),
                "tuples_per_second": float(m.group("tupsps")),
                "pipeline_tuples":   int(m.group("tuples").replace(",", ""))
            })

    return rows

def main(target):
    files = [target] if os.path.isfile(target) \
        else glob.glob(os.path.join(target, "**", "*.log"), recursive=True)

    all_rows = []
    for f in files:
        all_rows.extend(parse_log(f))
    if not all_rows:
        sys.exit("No data extracted.")

    df = pd.DataFrame(all_rows)
    df["total_time_s"] = df.groupby(
        ["query","memory_layout","buffer_bytes"])["time_s"].transform("sum")
    df["pct_of_total"] = df["time_s"] / df["total_time_s"] * 100

    out = os.path.join(os.path.dirname(os.path.abspath(target)), "results.csv")
    df.to_csv(out, index=False)
    print(f"Written results to `{out}`")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {os.path.basename(sys.argv[0])} path/to/log/or/dir")
        sys.exit(1)
    main(sys.argv[1])