import sys
import os
import re
from datetime import datetime
from collections import defaultdict

def compute_stats(stats_path):
    import re
    from datetime import datetime
    from collections import defaultdict

    start_re       = re.compile(r'(?P<ts>[\d\-]+ [\d:\.]+) Task (?P<tid>\d+) for Pipeline (?P<p>\d+) of')
    complete_re    = re.compile(r'(?P<ts>[\d\-]+ [\d:\.]+) Task (?P<tid>\d+) for Pipeline (?P<p>\d+) of.*Completed')
    tuple_re       = re.compile(r'Number of Tuples: (?P<t>\d+)')
    trans_re       = re.compile(
        r'(?P<ts>[\d\-]+ [\d:\.]+) Task \d+ for Pipeline (?P<p1>\d+) to Pipeline (?P<p2>\d+) of.*Number of Tuples: (?P<t>\d+)'
    )

    def parse_ts(raw):
        date, _, frac = raw.partition('.')
        frac = (frac + '000000')[:6]
        return datetime.fromisoformat(f'{date}.{frac}')

    starts, ends, tuples = {}, {}, {}
    transit = defaultdict(lambda: {'in': 0, 'out': 0})

    with open(stats_path) as f:
        for line in f:
            if 'to Pipeline' in line:
                m = trans_re.search(line)
                if m:
                    p1, p2, cnt = int(m.group('p1')), int(m.group('p2')), int(m.group('t'))
                    transit[p1]['out'] += cnt
                    transit[p2]['in']  += cnt
            elif 'Started' in line:
                m1, m2 = start_re.search(line), tuple_re.search(line)
                if m1 and m2:
                    key = (int(m1.group('p')), int(m1.group('tid')))
                    starts[key] = parse_ts(m1.group('ts'))
                    tuples[key] = int(m2.group('t'))
            elif 'Completed' in line:
                m = complete_re.search(line)
                if m:
                    key = (int(m.group('p')), int(m.group('tid')))
                    ends[key] = parse_ts(m.group('ts'))

    # only keep tasks with both start & end
    durations = {}
    throughputs = {}
    for key, ts0 in starts.items():
        if key in ends:
            dur = (ends[key] - ts0).total_seconds()
            if dur > 0:
                durations[key] = dur
                throughputs[key] = tuples[key] / dur

    # safe full-query time from matched tasks
    if durations:
        all_task_time  = sum(durations.values())
        sts = [starts[k] for k in durations]
        ets = [ends[k]   for k in durations]
        full_query_time = (max(ets) - min(sts)).total_seconds()
    else:
        all_task_time = full_query_time = 0.0

    # per-pipeline aggregates
    agg = {}
    for (pid, _), tp in throughputs.items():
        stats = agg.setdefault(pid, {
            'sum_tp': 0, 'count': 0, 'sum_tuples': 0, 'sum_time': 0
        })
        stats['sum_tp']     += tp
        stats['count']      += 1
        stats['sum_tuples'] += tuples[(pid, _)]
        stats['sum_time']   += durations[(pid, _)]

    # finalize metrics and selectivity
    for pid, s in agg.items():
        s['avg_tp']  = s['sum_tp'] / s['count']
        s['pct_time']= s['sum_time'] / all_task_time * 100 if all_task_time else 0
        inc = transit[pid]['in']
        out = transit[pid]['out']
        if inc and out:
            s['selectivity'] = out / inc * 100
        else:
            s['selectivity'] = 100.0

    return all_task_time, full_query_time, agg

def main():
    if len(sys.argv) != 2:
        script = os.path.basename(__file__)
        print(f"Usage: python `{script}` DirPath")
        sys.exit(1)

    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print(f"`{directory}` is not a directory")
        sys.exit(1)

    for filename in os.listdir(directory):
        path = os.path.join(directory, filename)
        if not os.path.isfile(path):
            continue

        total_time, full_time, pipelines = compute_stats(path)
        base, _ = os.path.splitext(filename)
        out_path = os.path.join(directory, f"{base}.txt")

        with open(out_path, 'w') as out:
            out.write(f"Total tasks time: {total_time:.2f}s\n")
            out.write(f"Full query duration: {full_time:.2f}s\n\n")
            for pid in sorted(pipelines):
                p = pipelines[pid]
                out.write(
                    f"Pipeline {pid}: avg_tp={p['avg_tp']:.2f} tuples/s, "
                    f"total_tuples={p['sum_tuples']}, "
                    f"time_pct={p['pct_time']:.2f}%\n"
                )

if __name__ == "__main__":
    main()