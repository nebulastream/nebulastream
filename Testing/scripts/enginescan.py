import sys
import os
import re
from datetime import datetime
from collections import defaultdict

def compute_throughput(stats_path):
    start_re     = re.compile(r'(?P<ts>[\d\-]+ [\d\:\.]+) Task \d+ for Pipeline (?P<p>\d+) of')
    complete_re  = re.compile(r'(?P<ts>[\d\-]+ [\d\:\.]+) Task \d+ for Pipeline (?P<p>\d+) of .*Completed')
    tuple_re     = re.compile(r'Number of Tuples: (?P<t>\d+)')

    first_start, last_end = {}, {}
    total_tuples = defaultdict(int)

    def parse_ts(raw_ts):
        date, _, frac = raw_ts.partition('.')
        frac = (frac + '000000')[:6]
        return datetime.fromisoformat(f'{date}.{frac}')

    with open(stats_path) as f:
        for line in f:
            if 'Started' in line:
                m1, m2 = start_re.search(line), tuple_re.search(line)
                if m1 and m2:
                    ts  = parse_ts(m1.group('ts'))
                    pid = int(m1.group('p'))
                    first_start.setdefault(pid, ts)
                    total_tuples[pid] += int(m2.group('t'))
            elif 'Completed' in line:
                m = complete_re.search(line)
                if m:
                    ts  = parse_ts(m.group('ts'))
                    pid = int(m.group('p'))
                    last_end[pid] = max(ts, last_end.get(pid, ts))

    throughputs = {}
    for pid, count in total_tuples.items():
        if pid in first_start and pid in last_end:
            dur = (last_end[pid] - first_start[pid]).total_seconds()
            throughputs[pid] = count / dur if dur > 0 else 0

    return throughputs, dict(total_tuples)

def main():
    if len(sys.argv) != 2:
        script = os.path.basename(__file__)
        print(f"Usage: python `{script}` AbsFilePath")
        sys.exit(1)

    throughputs, counts = compute_throughput(sys.argv[1])
    for pid in sorted(throughputs):
        print(f'Pipeline {pid}: {counts[pid]} tuples, {throughputs[pid]:.2f} tuples/s')

if __name__ == "__main__":
    main()