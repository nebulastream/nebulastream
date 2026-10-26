# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib", "plotly"]
# ///
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        https://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Stitches NES event trace files of the ring backend (nes-common/include/Util/EventTrace.hpp) into per-buffer latency breakdowns.

Every buffer that reaches a sink is walked back to the source buffer it came from, using the buffer key
(originId, sequenceNumber, chunkNumber), the Emit links recorded by the engine and the pipeline graph from the plan file.
Across a window, the walk follows the buffer that triggered the window, so the time spent waiting for the trigger is not part
of the latency. Across workers, NetSend/NetRecv events with the same key are matched.

    uv run scripts/latency_trace.py <dir-or-prefix> [--out <dir>]

Buffers in flight while tracing was turned on or off at runtime (nes-cli trace on|off) lack events and are counted as
incomplete.

Writes breakdown.svg (p50/p90/p99/max stacked by stage), worst.svg (timeline of the slowest buffer), trace.json
(open in https://ui.perfetto.dev) and report.html (interactive; pipeline graph, latency over time, stage distributions)
to the output directory.
"""

import argparse
import bisect
import json
import re
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

HEADER = struct.Struct("<8sQQQ64s")
EVENT = struct.Struct("<QQQQQQIIIBBH")
SOURCE_BACKPRESSURE, SOURCE_BUFFER_WAIT, SOURCE_FILL, ENQUEUE, EXEC, EMIT, NET_SEND, NET_RECV, COUNTER, TASK_QUEUE_WAIT = range(10)
STAGE_NAMES = [
    "source backpressure", "source buffer wait", "source fill", "enqueue", "exec", "emit", "net send", "net recv", "counter", "task queue wait"
]
POINT, BEGIN, END = range(3)
# A source that reports first data within this many ns of starting the fill found the data already waiting, so the data also
# waited through the preceding backpressure and buffer acquisition.
DATA_WAS_WAITING_NS = 50_000


class Incomplete(Exception):
    pass


@dataclass
class Worker:
    name: str
    pipelines: dict = field(default_factory=dict)  # id -> (successors, description)
    predecessors: dict = field(default_factory=lambda: defaultdict(set))
    source_fed: set = field(default_factory=set)  # pipelines fed directly by a source
    network_fed: set = field(default_factory=set)  # pipelines fed directly by a network source
    queries: dict = field(default_factory=dict)  # pipeline -> query name
    plan_sources: list = field(default_factory=list)  # (successors, description, query)
    siblings: dict = field(default_factory=dict)  # first successor of a source -> all its successors
    execs: dict = field(default_factory=lambda: defaultdict(list))  # (key, pipeline) -> [(t0, t1, tuples)]
    enqueues: dict = field(default_factory=lambda: defaultdict(list))  # (key, pipeline) -> [(t0, t1)]
    emits: dict = field(default_factory=lambda: defaultdict(list))  # key -> [(t, input key, pipeline)]
    sources: dict = field(default_factory=lambda: defaultdict(dict))  # (key, pipeline) -> stage -> (t0, t1, seq2)
    receives: dict = field(default_factory=dict)  # (key, pipeline) -> t
    open_spans: list = field(default_factory=list)  # (stage, thread, key, pipeline, t) of begins without an end
    unpaired: int = 0  # ends without a begin and begins without an end that a later begin superseded
    end: int = 0  # the last event's time
    counter_names: dict = field(default_factory=dict)  # counter id -> name
    counters: dict = field(default_factory=lambda: defaultdict(list))  # counter id -> [(t, value)], recorded on change

    def label(self, pipeline):
        return f"P{pipeline} {short_description(self.pipelines.get(pipeline, ((), '?'))[1])}"

    def is_network_sink(self, pipeline):
        return self.pipelines.get(pipeline, ((), ""))[1].startswith("NetworkSink")


def pipeline_node(worker, pipeline):
    return f"{worker.name}/P{pipeline}"


def source_node(worker, pipeline):
    """A source is drawn once per pipeline it feeds."""
    return f"{worker.name}/S{pipeline}"


def latest(entries, limit, time=lambda entry: entry[0]):
    candidates = [entry for entry in entries if time(entry) <= limit]
    if not candidates:
        raise Incomplete
    return max(candidates, key=time)


def load(paths):
    workers, sends = [], defaultdict(list)
    for path in paths:
        data = path.read_bytes()
        magic, pid, steady, system, host = HEADER.unpack_from(data)
        if magic != b"NESLAT02":
            sys.exit(f"{path}: not a latency trace")
        worker = Worker(f"{host.rstrip(b'\0').decode()}:{pid}")
        plan = path.with_suffix(".plan")
        if plan.exists():
            query = "?"
            for line in plan.read_text().splitlines():
                kind, _, rest = line.partition(" ")
                if kind == "query":
                    query = query_name(rest)
                if kind == "counter":
                    identifier, name = rest.split(" ", 1)
                    worker.counter_names[int(identifier)] = name
                if kind not in ("source", "pipeline"):
                    continue
                identifier, successors, description = rest.split(" ", 2)
                successors = [] if successors == "-" else [int(s) for s in successors.split(",")]
                if kind == "source":
                    worker.plan_sources.append((successors, description, query))
                    if successors:
                        worker.siblings[successors[0]] = successors
                    fed = worker.network_fed if "NetworkSource" in description else worker.source_fed
                    fed.update(successors)
                    continue
                pipeline = identifier
                worker.pipelines[int(pipeline)] = (successors, description)
                worker.queries[int(pipeline)] = query
                for successor in successors:
                    worker.predecessors[successor].add(int(pipeline))
        load_events(worker, data[HEADER.size :], system - steady, sends)
        workers.append(worker)
    return workers, sends


def load_events(worker, data, offset, sends):
    """Pairs the begins and ends of each thread into spans. Spans of one stage do not overlap on a thread, so an end closes the
    last begin of its stage. Source backpressure and buffer wait end without a key; they belong to the next fill on the thread."""
    opened, source_waits = {}, defaultdict(dict)
    for time, _, origin, seq, origin2, seq2, chunk, chunk2, pipeline, stage, kind, thread in EVENT.iter_unpack(data):
        key, time = (origin, seq, chunk), time + offset
        worker.end = max(worker.end, time)
        if kind == BEGIN:
            if (thread, stage) in opened:
                worker.unpaired += 1
            opened[thread, stage] = (time, key, pipeline)
            continue
        if kind == END:
            begin = opened.pop((thread, stage), None)
            if begin is None:
                worker.unpaired += 1
                continue
            t0 = begin[0]
            if stage == EXEC:
                worker.execs[key, pipeline].append((t0, time, seq2))
            elif stage == ENQUEUE:
                worker.enqueues[key, pipeline].append((t0, time))
            elif stage in (SOURCE_BACKPRESSURE, SOURCE_BUFFER_WAIT):
                source_waits[thread][stage] = (t0, time, 0)
            elif stage == SOURCE_FILL and key != (0, 0, 0):
                stages = {**source_waits.pop(thread, {}), SOURCE_FILL: (t0, time, seq2 + offset if seq2 else 0)}
                for successor in worker.siblings.get(pipeline, [pipeline]):
                    worker.sources[key, successor] = stages
            continue
        if stage == EMIT:
            worker.emits[key].append((time, (origin2, seq2, chunk2), pipeline))
        elif stage == NET_SEND:
            sends[key].append((time, worker))
        elif stage == NET_RECV:
            worker.receives[key, pipeline] = time
        elif stage == COUNTER:
            worker.counters[pipeline].append((time, struct.unpack("<q", struct.pack("<Q", seq2))[0]))
    # A worker that waits for its next task when the trace ends is idle, not stuck.
    worker.open_spans = [
        (stage, thread, key, pipeline, t) for (thread, stage), (t, key, pipeline) in opened.items() if stage != TASK_QUEUE_WAIT
    ]


def warmup_end(workers):
    """When the last pipeline finished its first execution. First executions pay one-time costs, such as faulting in the
    compiled code, and buffers that arrive meanwhile queue up behind them."""
    first = {}
    for worker in workers:
        for (_, pipeline), execs in worker.execs.items():
            start, end, _ = min(execs)
            if start < first.get((worker.name, pipeline), (float("inf"), 0))[0]:
                first[worker.name, pipeline] = (start, end)
    return max((end for _, end in first.values()), default=0)


def walk(worker, key, pipeline, end, sends):
    """Segments (name, t0, t1, node) from the source buffer of `key` until `pipeline` finished with it at `end`.
    `node` is the id of the pipeline, source or network edge in the report's graph that the segment belongs to."""
    execs = worker.execs.get((key, pipeline), [])
    if worker.is_network_sink(pipeline):
        # A full network channel repeats the sink task; the first execution is when the buffer reached the sink.
        execution = min((e for e in execs if e[0] <= end), default=None)
        if execution is None:
            raise Incomplete
        name = f"network sink + retries {worker.label(pipeline)}"
    else:
        execution = latest(execs, end)
        name = f"exec {worker.label(pipeline)}"
    enqueue = latest(worker.enqueues.get((key, pipeline), []), execution[0])
    label, node = worker.label(pipeline), pipeline_node(worker, pipeline)
    # The handoff starts where the previous segment ended, e.g. a statistic listener between emit and enqueue.
    segments = [
        (f"handoff {label}", enqueue[0], enqueue[0], node),
        (f"enqueue {label}", enqueue[0], enqueue[1], node),
        (f"queue {label}", enqueue[1], execution[0], node),
        (name, execution[0], end, node),
    ]

    emits = [e for e in worker.emits.get(key, []) if e[2] in worker.predecessors[pipeline]]
    if emits:
        emitted, input_key, from_pipeline = latest(emits, enqueue[0])
        return walk(worker, input_key, from_pipeline, emitted, sends) + segments

    if pipeline in worker.network_fed and (key, pipeline) in worker.receives:
        received = worker.receives[key, pipeline]
        sent, sender = latest(sends.get(key, []), received)
        sink = next((p for p in sender.pipelines if sender.is_network_sink(p) and (key, p) in sender.execs), None)
        if sink is None:
            raise Incomplete
        source = source_node(worker, pipeline)
        hop = [
            ("network", sent, received, f"{pipeline_node(sender, sink)}>{source}"),
            ("network source to engine", received, enqueue[0], source),
        ]
        return walk(sender, key, sink, sent, sends) + hop + segments

    # Without an Emit link the buffer was produced outside of a task, e.g. a window flushed at end of stream.
    stages = worker.sources.get((key, pipeline))
    if pipeline not in worker.source_fed or not stages or SOURCE_FILL not in stages:
        raise Incomplete
    fill_start, emitted, first_data = stages[SOURCE_FILL]
    node = source_node(worker, pipeline)
    if first_data and first_data - fill_start > DATA_WAS_WAITING_NS:
        source = [("source fill (batching)", first_data, emitted, node)]
    elif first_data:
        if SOURCE_BACKPRESSURE not in stages or SOURCE_BUFFER_WAIT not in stages:
            raise Incomplete
        backpressure, buffer = stages[SOURCE_BACKPRESSURE], stages[SOURCE_BUFFER_WAIT]
        source = [
            ("source backpressure", backpressure[0], backpressure[1], node),
            ("source buffer wait", backpressure[1], buffer[1], node),
            ("source fill (batching)", buffer[1], emitted, node),
        ]
    else:
        source = [("source fill (batching)", fill_start, emitted, node)]
    return source + [("source in-flight limit", emitted, enqueue[0], node)] + segments


def stitch(workers, sends, skip_warmup=True, keep_warmup=False):
    """With keep_warmup, warm-up chains are kept and marked instead of dropped."""
    chains, incomplete, warmup = [], 0, 0
    warm = warmup_end(workers) if skip_warmup or keep_warmup else 0
    for worker in workers:
        sinks = {p for p, (successors, _) in worker.pipelines.items() if not successors and not worker.is_network_sink(p)}
        for (key, pipeline), execs in worker.execs.items():
            if pipeline not in sinks:
                continue
            for _, end, tuples in execs:
                try:
                    segments = walk(worker, key, pipeline, end, sends)
                except Incomplete:
                    incomplete += 1
                    continue
                if segments[0][1] < warm and not keep_warmup:
                    warmup += 1
                    continue
                # Consecutive segments share their boundary; rounding gaps go to the later segment.
                stitched, previous = [], segments[0][1]
                for name, _, t1, node in segments:
                    stitched.append((name, previous, max(previous, t1), node))
                    previous = max(previous, t1)
                # Sub-microsecond handoffs and enqueues only clutter the charts.
                stitched = [s for s in stitched if s[2] - s[1] >= 1000 or not s[0].startswith(("handoff", "enqueue"))]
                chains.append({
                    "key": key,
                    "tuples": max(tuples, 1),
                    "segments": [s[:3] for s in stitched],
                    "nodes": [s[3] for s in stitched],
                    "query": worker.queries.get(pipeline, "?"),
                    "warmup": segments[0][1] < warm,
                })
    return chains, incomplete, warmup


def query_name(description):
    """The distributed name groups the fragments of one query, which have different local ids."""
    _, found, name = description.partition("distributed=")
    return name.rstrip(")") if found else description


def total(chain):
    return chain["segments"][-1][2] - chain["segments"][0][1]


def percentile_index(chains, percentile):
    """Chains are sorted by latency and weighted by their number of tuples, so this is a per-tuple percentile."""
    cumulative, running = [], 0
    for chain in chains:
        running += chain["tuples"]
        cumulative.append(running)
    return min(bisect.bisect_left(cumulative, percentile / 100 * running), len(chains) - 1)


def mean_breakdown(chains):
    sums, order = defaultdict(float), []
    for chain in chains:
        for name, t0, t1 in chain["segments"]:
            if name not in sums:
                order.append(name)
            sums[name] += (t1 - t0) / 1e6
    return [(name, sums[name] / len(chains)) for name in order]


def report(chains, out):
    chains.sort(key=total)
    rows = {}
    for label, low, high in (("p50", 45, 55), ("p90", 85, 95), ("p99", 99, 100)):
        value = total(chains[percentile_index(chains, int(label[1:]))])
        band = chains[percentile_index(chains, low) : percentile_index(chains, high) + 1]
        rows[f"{label} ({value / 1e6:.2f} ms)"] = mean_breakdown(band)
    rows[f"max ({total(chains[-1]) / 1e6:.2f} ms)"] = mean_breakdown(chains[-1:])

    for label, breakdown in rows.items():
        print(f"\n{label}")
        for name, ms in breakdown:
            print(f"  {ms:10.3f} ms  {name}")

    import matplotlib

    matplotlib.use("Agg")
    # Keep labels as <text> elements so the SVGs stay searchable and copyable.
    matplotlib.rcParams["svg.fonttype"] = "none"
    import matplotlib.pyplot as plt

    names = list(dict.fromkeys(name for breakdown in rows.values() for name, _ in breakdown))
    palette = [*plt.cm.tab20.colors, *plt.cm.tab20b.colors]
    colors = {name: palette[i % len(palette)] for i, name in enumerate(names)}
    # The legend below the bars takes one line per two stages.
    legend_height = 0.17 * ((len(names) + 1) // 2)
    figure, axis = plt.subplots(figsize=(12, 1.2 + 0.6 * len(rows) + legend_height), layout="constrained")
    for y, (label, breakdown) in enumerate(rows.items()):
        left = 0.0
        for name, ms in breakdown:
            axis.barh(y, ms, left=left, color=colors[name], label=name, edgecolor="white", linewidth=0.5)
            left += ms
    handles, labels = axis.get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    figure.legend(unique.values(), unique.keys(), loc="outside lower center", ncols=2, fontsize=8, frameon=False)
    axis.set_yticks(range(len(rows)), list(rows))
    axis.invert_yaxis()
    axis.set_xlabel("ms (mean breakdown of buffers around the percentile)")
    figure.savefig(out / "breakdown.svg", bbox_inches="tight")

    worst = chains[-1]["segments"]
    start = worst[0][1]
    figure, axis = plt.subplots(figsize=(12, 0.5 + 0.3 * len(worst)))
    for y, (name, t0, t1) in enumerate(worst):
        axis.barh(y, (t1 - t0) / 1e6, left=(t0 - start) / 1e6, color=colors.get(name, "gray"))
    axis.set_yticks(range(len(worst)), [f"{name}  {(t1 - t0) / 1e6:.3f} ms" for name, t0, t1 in worst], fontsize=8)
    axis.invert_yaxis()
    axis.set_xlabel("ms since first data")
    figure.tight_layout()
    figure.savefig(out / "worst.svg", bbox_inches="tight")


def short_description(description):
    """`TCPSource(...)` inside a SourceThread description, else the first word of the stage description."""
    implementation = re.search(r"implementation: ?(\w+)", description)
    if implementation:
        return implementation.group(1)
    name = re.match(r"\w+", description)
    if name is None or name.group(0) == "CompiledExecutablePipelineStage":
        return "compiled pipeline"
    return name.group(0)


def graph(workers, queries):
    """Sources, pipelines and sinks of `queries`, with network edges between query fragments. The plan file lists every
    query, because tracing can be turned on while a query runs."""
    nodes, edges, channels = [], [], {}
    for worker in workers:
        for pipeline, (successors, description) in worker.pipelines.items():
            if worker.queries[pipeline] not in queries:
                continue
            kind = "network sink" if worker.is_network_sink(pipeline) else "sink" if not successors else "pipeline"
            nodes.append({"id": pipeline_node(worker, pipeline), "label": f"P{pipeline}", "what": short_description(description),
                          "detail": description, "query": worker.queries[pipeline], "kind": kind, "worker": worker.name})
            edges += [{"from": pipeline_node(worker, pipeline), "to": pipeline_node(worker, successor)} for successor in successors]
        for successors, description, query in worker.plan_sources:
            if query not in queries:
                continue
            network = re.search(r"NetworkSource\(([^)]+)\)", description)
            for successor in successors:
                node = source_node(worker, successor)
                nodes.append({"id": node, "label": "source", "what": short_description(description), "detail": description,
                              "query": query, "kind": "network source" if network else "source", "worker": worker.name})
                edges.append({"from": node, "to": pipeline_node(worker, successor)})
                if network:
                    channels[network.group(1)] = node
    for worker in workers:
        for pipeline, (_, description) in worker.pipelines.items():
            if worker.queries[pipeline] not in queries:
                continue
            channel = re.search(r"channelId: ([\w-]+)", description)
            if worker.is_network_sink(pipeline) and channel and channel.group(1) in channels:
                sink, source = pipeline_node(worker, pipeline), channels[channel.group(1)]
                edges.append({"from": sink, "to": source, "id": f"{sink}>{source}", "network": True})
    return {"nodes": nodes, "edges": edges}


def open_spans(workers):
    """Begins without an end, oldest first: (stage, pipeline, worker/thread, key, open for ms)."""
    spans = []
    for worker in workers:
        for stage, thread, key, pipeline, t in worker.open_spans:
            label = worker.label(pipeline) if pipeline else "-"
            key_text = "/".join(map(str, key)) if key != (0, 0, 0) else "-"
            spans.append((STAGE_NAMES[stage], label, f"{worker.name}/{thread}", key_text, (worker.end - t) / 1e6))
    return sorted(spans, key=lambda span: -span[4])


def counter_series(workers):
    """(name, worker, [(t, value)]) of every counter that recorded a sample. Values only are recorded when they change, so the last
    one is repeated at the end of the trace."""
    return [(worker.counter_names.get(identifier, f"counter {identifier}"), worker.name, samples + [(worker.end, samples[-1][1])])
            for worker in workers for identifier, samples in worker.counters.items()]


def interactive_report(workers, chains, out):
    """Writes report.html, a self-contained page that embeds the graph and the stitched chains."""
    stages, nodes, queries = {}, {}, {}
    start = min(chain["segments"][0][1] for chain in chains)
    rows = []
    for chain in chains:
        segments = [[stages.setdefault(name, len(stages)), nodes.setdefault(node, len(nodes)), round((t1 - t0) / 1000)]
                    for (name, t0, t1), node in zip(chain["segments"], chain["nodes"])]
        rows.append([queries.setdefault(chain["query"], len(queries)), round((chain["segments"][0][1] - start) / 1e6, 3),
                     chain["tuples"], int(chain["warmup"]), "/".join(map(str, chain["key"])), segments])
    data = {"stages": list(stages), "nodes": list(nodes), "queries": list(queries), "chains": rows, "graph": graph(workers, {chain["query"] for chain in chains}),
            "open": open_spans(workers),
            "counters": [{"name": name, "worker": where, "t": [round((t - start) / 1e6, 3) for t, _ in samples],
                          "v": [value for _, value in samples]} for name, where, samples in counter_series(workers)]}
    from plotly.offline import get_plotlyjs

    template = (Path(__file__).parent / "latency_trace_report.html").read_text()
    page = template.replace("/*TRACE_DATA*/null", json.dumps(data, separators=(",", ":"))).replace("/*PLOTLY_JS*/", get_plotlyjs())
    (out / "report.html").write_text(page)


def perfetto(chains, out, count=100):
    """The slowest and a sample of the remaining buffers, one track per buffer."""
    picked = chains[-count // 2 :] + chains[: -count // 2 : max(1, len(chains) // (count // 2))]
    events = []
    for track, chain in enumerate(picked):
        name = f"{total(chain) / 1e6:.2f} ms origin {chain['key'][0]} seq {chain['key'][1]} chunk {chain['key'][2]}"
        events.append({"ph": "M", "name": "thread_name", "pid": 1, "tid": track, "args": {"name": name}})
        for segment, t0, t1 in chain["segments"]:
            events.append({"ph": "X", "name": segment, "pid": 1, "tid": track, "ts": t0 / 1000, "dur": (t1 - t0) / 1000})
    (out / "trace.json").write_text(json.dumps({"traceEvents": events}))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", type=Path, help="directory containing *.bin traces, or a trace prefix")
    parser.add_argument("--out", type=Path, default=Path("."))
    parser.add_argument("--query", help="only buffers of queries whose name contains this")
    parser.add_argument("--keep-warmup", action="store_true", help="keep buffers that started before every pipeline ran once")
    args = parser.parse_args()
    if args.input.is_dir():
        paths = sorted(args.input.glob("*.bin"))
    else:
        paths = sorted(args.input.parent.glob(f"{args.input.name}.*.bin"))
    if not paths:
        sys.exit(f"no traces found for {args.input}")

    workers, sends = load(paths)
    everything, _, _ = stitch(workers, sends, keep_warmup=True)
    chains, incomplete, warmup = stitch(workers, sends, skip_warmup=not args.keep_warmup)
    print(f"{len(paths)} trace files, {len(chains)} complete chains, {incomplete} incomplete, {warmup} skipped as warm-up")
    by_query = defaultdict(list)
    for chain in chains:
        by_query[chain["query"]].append(chain)
    for query, own in sorted(by_query.items()):
        own.sort(key=total)
        p50, p99 = (total(own[percentile_index(own, p)]) / 1e6 for p in (50, 99))
        print(f"  {query}: {len(own)} buffers, p50 {p50:.2f} ms, p99 {p99:.2f} ms")
    for name, where, samples in counter_series(workers):
        values = [value for _, value in samples]
        print(f"counter {name} on {where}: {len(values)} changes, min {min(values)}, max {max(values)}")
    spans = open_spans(workers)
    unpaired = sum(worker.unpaired for worker in workers)
    print(f"{len(spans)} spans still open at the end of the trace, {unpaired} unpaired begins or ends")
    for stage, pipeline, where, key, age in spans[:10]:
        print(f"  {age:10.1f} ms  {stage} {pipeline} on {where}, buffer {key}")
    if args.query is not None:
        chains = [chain for chain in chains if args.query in chain["query"]]
    if not chains:
        sys.exit(1)
    args.out.mkdir(parents=True, exist_ok=True)
    report(chains, args.out)
    perfetto(chains, args.out)
    interactive_report(workers, everything, args.out)
    print(f"\nwrote {args.out / 'breakdown.svg'}, {args.out / 'worst.svg'}, {args.out / 'trace.json'}, {args.out / 'report.html'}")


if __name__ == "__main__":
    main()
