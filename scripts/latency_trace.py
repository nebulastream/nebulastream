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
CHUNK = struct.Struct("<HI")  # thread, length
RECORD = struct.Struct("<HBQ")  # type, kind, time
BEGIN, END, INSTANT = range(3)
# Field types of the schema: a buffer key, uint64_t and int64_t.
FIELD_FORMATS = {"k": "QQQ", "u": "Q", "i": "q"}
# Event type names, see the structs in nes-common/include/Util/EventTrace.hpp.
SOURCE_BACKPRESSURE, SOURCE_BUFFER_WAIT, SOURCE_FILL = "source backpressure", "source buffer wait", "source fill"
TASK_QUEUE_WAIT = "task queue wait"
# Steps of a traced buffer through the network layer; dequeue, encoded and acked happen on the sender, decoded and queued on the
# receiver.
NET_STEPS = ("net dequeue", "net encoded", "net acked", "net decoded", "net queued")
# Gaps between the spans of a worker thread, which executes tasks of untraced queries or engine work in them.
UNTRACED_WORK, UNTRACED = "untraced work", 3
# Thread activity is exported for this many of the slowest buffers and as many others, to bound the report's size.
ACTIVITY_SLOWEST = ACTIVITY_SAMPLED = 200
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
    spans: dict = field(default_factory=lambda: defaultdict(list))  # thread -> [(t0, t1, name, category, key, pipeline, tuples)] by t0
    net_steps: dict = field(default_factory=lambda: defaultdict(list))  # (step name, key, channel) -> [t], see NET_STEPS
    sink_channels: dict = field(default_factory=dict)  # network sink pipeline -> channel tag
    clock_offset: int = 0  # system clock minus steady clock, added to every event time
    thread_ids: dict = field(default_factory=dict)  # trace thread -> (OS thread id, thread name)
    cpu: dict = field(default_factory=lambda: defaultdict(list))  # trace thread -> [(t0, t1, CPU state)], see load_sched
    source_channels: dict = field(default_factory=dict)  # pipeline fed by a network source -> channel tag
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


@dataclass
class EventType:
    name: str
    category: int  # EventTrace::Category: 0 work, 1 wait, 2 other
    size: int
    fields: list  # (name, type) in payload order
    layout: struct.Struct

    def decode(self, data, offset):
        values, fields = iter(self.layout.unpack_from(data, offset)), {}
        for name, kind in self.fields:
            fields[name] = (next(values), next(values), next(values)) if kind == "k" else next(values)
        return fields


def channel_tag(channel_id):
    """EventTrace::channelTag: FNV-1a of the channel id, never 0."""
    tag = 0xCBF29CE484222325
    for byte in channel_id.encode():
        tag = ((tag ^ byte) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return tag or 1


def read_name(data, offset):
    (length,) = struct.unpack_from("<H", data, offset)
    return data[offset + 2 : offset + 2 + length].decode(), offset + 2 + length


def read_schemas(data, offset):
    """The event types the trace was written with, by id, and the offset of the first chunk."""
    (count,) = struct.unpack_from("<H", data, offset)
    offset += 2
    types = {}
    for _ in range(count):
        (type_id,) = struct.unpack_from("<H", data, offset)
        name, offset = read_name(data, offset + 2)
        category, size, field_count = struct.unpack_from("<BIH", data, offset)
        offset += 7
        fields = []
        for _ in range(field_count):
            field, offset = read_name(data, offset)
            kind, field_offset = struct.unpack_from("<cI", data, offset)
            offset += 5
            fields.append((field_offset, field, kind.decode()))
        layout, position = "<", 0
        for field_offset, _, kind in sorted(fields):
            layout += "x" * (field_offset - position) + FIELD_FORMATS[kind]
            position = field_offset + struct.calcsize("<" + FIELD_FORMATS[kind])
        layout += "x" * (size - position)
        types[type_id] = EventType(name, category, size, [(f, k) for _, f, k in sorted(fields)], struct.Struct(layout))
    return types, offset


def load(paths):
    workers, sends = [], defaultdict(list)
    for path in paths:
        data = path.read_bytes()
        magic, pid, steady, system, host = HEADER.unpack_from(data)
        if magic != b"NESLAT03":
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
                if kind == "thread":
                    thread, os_thread, name = (rest.split(" ", 2) + [""])[:3]
                    worker.thread_ids[int(thread)] = (int(os_thread), name)
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
                    if channel := re.search(r"NetworkSource\(([^)]+)\)", description):
                        worker.source_channels.update({successor: channel_tag(channel.group(1)) for successor in successors})
                    continue
                pipeline = identifier
                worker.pipelines[int(pipeline)] = (successors, description)
                if channel := re.search(r"channelId: ([\w-]+)", description):
                    worker.sink_channels[int(pipeline)] = channel_tag(channel.group(1))
                worker.queries[int(pipeline)] = query
                for successor in successors:
                    worker.predecessors[successor].add(int(pipeline))
        types, offset = read_schemas(data, HEADER.size)
        worker.clock_offset = system - steady
        load_events(worker, types, data, offset, system - steady, sends)
        workers.append(worker)
    if (sched := paths[0].parent / "sched.txt").exists():
        load_sched(sched, workers)
    return workers, sends


RUNNING, RUNNABLE, WAKE_REQUESTED = 0, 1, 2
CPU_STATES = ["running", "waiting for a CPU", "wake requested"]
SCHED_LINE = re.compile(r"^\s*(.+)-(\d+)\s+\[\d+\]\s+\S+\s+(\d+\.\d+): (sched_switch|sched_waking|sched_wakeup): (.*)$")


def load_sched(path, workers):
    """When the traced threads ran, waited for a CPU and were being woken, from scripts/latency_sched.sh: (t0, t1, CPU state,
    waking thread) per trace thread. A sleeping thread is "wake requested" from sched_waking, when another thread asks to wake it,
    until sched_wakeup, when it becomes runnable; a woken or preempted thread waits for a CPU until it is switched in; a thread that
    switched out otherwise sleeps. Lines can be cut off and glued to the next record, so such lines are skipped, and a transition
    that implies a lost record drops the interval instead of stretching it."""
    threads = {os_thread: (worker, thread) for worker in workers for thread, (os_thread, _) in worker.thread_ids.items()}
    names = {os_thread: f"{name} ({worker.name})" for worker in workers for os_thread, name in worker.thread_ids.values()}
    state = {}  # OS thread id -> (CPU state or None for sleeping, since, waking thread)

    def leave(os_thread, time, expected):
        current, since, waker = state.get(os_thread, (None, time, ""))
        if current == expected and os_thread in threads:
            worker, thread = threads[os_thread]
            worker.cpu[thread].append((since + worker.clock_offset, time + worker.clock_offset, current, waker))

    skipped = 0
    with path.open() as lines:
        for line in lines:
            match = SCHED_LINE.match(line)
            if not match or line.count(": sched_") > 1:
                skipped += 1
                continue
            comm, current, time, event, fields = match.group(1).strip(), int(match.group(2)), round(float(match.group(3)) * 1e9), match.group(4), match.group(5)
            if event != "sched_switch":
                woken = int(re.search(r" pid=(\d+)", " " + fields).group(1))
                if event == "sched_waking":
                    if state.get(woken, (None,))[0] is None:
                        state[woken] = (WAKE_REQUESTED, time, names.get(current, f"{comm}-{current}"))
                    continue
                leave(woken, time, WAKE_REQUESTED)
                if state.get(woken, (None,))[0] != RUNNABLE:
                    state[woken] = (RUNNABLE, time, "")
                continue
            previous = int(re.search(r"prev_pid=(\d+)", fields).group(1))
            next_thread = int(re.search(r"next_pid=(\d+)", fields).group(1))
            preempted = re.search(r"prev_state=(\S+)", fields).group(1).startswith("R")
            leave(previous, time, RUNNING)
            state[previous] = (RUNNABLE if preempted else None, time, "")
            leave(next_thread, time, RUNNABLE)
            leave(next_thread, time, WAKE_REQUESTED)
            state[next_thread] = (RUNNING, time, "")
    for spans in (worker.cpu for worker in workers):
        for intervals in spans.values():
            intervals.sort()
    if skipped:
        print(f"{path}: skipped {skipped} unreadable scheduling records")


def records(types, data, offset):
    """(thread, kind, time, event type, fields) of every record, in the order each thread recorded them."""
    while offset < len(data):
        thread, length = CHUNK.unpack_from(data, offset)
        offset += CHUNK.size
        end = offset + length
        while offset < end:
            type_id, kind, time = RECORD.unpack_from(data, offset)
            event = types[type_id]
            yield thread, kind, time, event, event.decode(data, offset + RECORD.size)
            offset += RECORD.size + event.size


def load_events(worker, types, data, offset, clock_offset, sends):
    """Pairs the begins and ends of each thread into spans. Spans of one type do not overlap on a thread, so an end closes the
    last begin of its type. Source backpressure and buffer wait have no key; they belong to the next fill on the thread."""
    opened, source_waits = {}, defaultdict(dict)
    types_by_name = {event.name: event for event in types.values()}
    for thread, kind, time, event, fields in records(types, data, offset):
        time += clock_offset
        worker.end = max(worker.end, time)
        name = event.name
        if kind == BEGIN:
            if (thread, name) in opened:
                worker.unpaired += 1
            opened[thread, name] = (time, fields)
            continue
        if kind == END:
            begin = opened.pop((thread, name), None)
            if begin is None:
                worker.unpaired += 1
                continue
            t0 = begin[0]
            worker.spans[thread].append(
                (t0, time, name, event.category, fields.get("key", (0, 0, 0)), fields.get("pipeline", 0), fields.get("tuples", 0)))
            if name == "exec":
                worker.execs[fields["key"], fields["pipeline"]].append((t0, time, fields["tuples"]))
            elif name == "enqueue":
                worker.enqueues[fields["key"], fields["pipeline"]].append((t0, time))
            elif name in (SOURCE_BACKPRESSURE, SOURCE_BUFFER_WAIT):
                source_waits[thread][name] = (t0, time, 0)
            elif name == SOURCE_FILL and fields["key"] != (0, 0, 0):
                first_data = fields["firstData"] + clock_offset if fields["firstData"] else 0
                stages = {**source_waits.pop(thread, {}), SOURCE_FILL: (t0, time, first_data)}
                for successor in worker.siblings.get(fields["pipeline"], [fields["pipeline"]]):
                    worker.sources[fields["key"], successor] = stages
            continue
        if name == "emit":
            worker.emits[fields["output"]].append((time, fields["input"], fields["pipeline"]))
        elif name == "net send":
            sends[fields["key"], fields["channel"]].append((time, worker))
        elif name == "net recv":
            received = fields["received"] + clock_offset if fields["received"] else time
            worker.receives[fields["key"], fields["pipeline"]] = received
        elif name == "counter":
            worker.counters[fields["counter"]].append((time, fields["value"]))
        elif name in NET_STEPS:
            worker.net_steps[name, fields["key"], fields["channel"]].append(time)
    for (thread, name), (t, fields) in opened.items():
        worker.spans[thread].append((t, worker.end, name, types_by_name[name].category, fields.get("key", (0, 0, 0)),
                                     fields.get("pipeline", 0), fields.get("tuples", 0)))
    for spans in worker.spans.values():
        spans.sort()
    # A worker that waits for its next task when the trace ends is idle, not stuck.
    worker.open_spans = [(name, thread, fields.get("key", (0, 0, 0)), fields.get("pipeline", 0), t)
                         for (thread, name), (t, fields) in opened.items() if name != TASK_QUEUE_WAIT]


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


def walk(worker, key, pipeline, end, sends, tasks):
    """Segments (name, t0, t1, node) from the source buffer of `key` until `pipeline` finished with it at `end`.
    `node` is the id of the pipeline, source or network edge in the report's graph that the segment belongs to.
    Adds the (worker name, key, pipeline) of every task on the way to `tasks`."""
    tasks.add((worker.name, key, pipeline))
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
    # A worker can start the task before the enqueuing thread returns from the enqueue call, e.g. when the enqueuing thread loses
    # its CPU after waking the worker. The enqueue then ends where the execution begins.
    queued = min(enqueue[1], execution[0])
    # The handoff starts where the previous segment ended, e.g. a statistic listener between emit and enqueue.
    segments = [
        (f"handoff {label}", enqueue[0], enqueue[0], node),
        (f"enqueue {label}", enqueue[0], queued, node),
        (f"queue {label}", queued, execution[0], node),
        (name, execution[0], end, node),
    ]

    emits = [e for e in worker.emits.get(key, []) if e[2] in worker.predecessors[pipeline]]
    if emits:
        emitted, input_key, from_pipeline = latest(emits, enqueue[0])
        return walk(worker, input_key, from_pipeline, emitted, sends, tasks) + segments

    if pipeline in worker.network_fed and (key, pipeline) in worker.receives:
        received = worker.receives[key, pipeline]
        # Buffers of different queries can have the same key, so the channel decides which send this buffer came from.
        channel = worker.source_channels.get(pipeline)
        sent, sender = latest(sends.get((key, channel), []), received)
        sink = next((p for p, tag in sender.sink_channels.items() if tag == channel), None)
        if sink is None:
            raise Incomplete
        source = source_node(worker, pipeline)
        hop = network_hop(sender, worker, key, channel, sent, received, f"{pipeline_node(sender, sink)}>{source}", sender.label(sink))
        hop.append((f"network source to engine {label}", received, enqueue[0], source))
        return walk(sender, key, sink, sent, sends, tasks) + hop + segments

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


def network_hop(sender, receiver, key, channel, sent, received, edge, sink_label):
    """Segments from NetSend on the sender to the network source taking the buffer on the receiver. Without the network
    layer's steps, e.g. for untraced resends, the hop is a single segment."""
    def first(worker, step, after):
        return min((t for t in worker.net_steps.get((step, key, channel), []) if after <= t <= received), default=None)

    dequeued = first(sender, "net dequeue", sent)
    encoded = dequeued and first(sender, "net encoded", dequeued)
    queued = max((t for t in receiver.net_steps.get(("net queued", key, channel), []) if t <= received), default=None)
    decoded = queued and max((t for t in receiver.net_steps.get(("net decoded", key, channel), []) if t <= queued), default=None)
    if not (dequeued and encoded and decoded and queued):
        return [(f"network {sink_label}", sent, received, edge)]
    # Named after the sending network sink, so the hops of a buffer through several workers stay apart.
    return [
        (f"net sender queue {sink_label}", sent, dequeued, edge),
        (f"net encode {sink_label}", dequeued, encoded, edge),
        (f"net transfer {sink_label}", encoded, decoded, edge),
        (f"net receiver handoff {sink_label}", decoded, queued, edge),
        (f"net receiver queue {sink_label}", queued, received, edge),
    ]


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
                tasks = set()
                try:
                    segments = walk(worker, key, pipeline, end, sends, tasks)
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
                    "tasks": tasks,
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
            spans.append((stage, label, f"{worker.name}/{thread}", key_text, (worker.end - t) / 1e6))
    return sorted(spans, key=lambda span: -span[4])


def counter_series(workers):
    """(name, worker, [(t, value)]) of every counter that recorded a sample. Values only are recorded when they change, so the last
    one is repeated at the end of the trace."""
    return [(worker.counter_names.get(identifier, f"counter {identifier}"), worker.name, samples + [(worker.end, samples[-1][1])])
            for worker in workers for identifier, samples in worker.counters.items()]


def thread_role(spans):
    names = {span[2] for span in spans}
    return "worker" if TASK_QUEUE_WAIT in names else "source" if names & {SOURCE_BACKPRESSURE, SOURCE_BUFFER_WAIT, SOURCE_FILL} else "other"


def chain_workers(chain, by_name):
    """The workers a chain passed through, in order. Network nodes name both ends."""
    involved = []
    for node in chain["nodes"]:
        for part in node.split(">"):
            worker = by_name[part.split("/")[0]]
            if worker not in involved:
                involved.append(worker)
    return involved


def thread_activity(chain, workers, span_names):
    """What the threads of the chain's workers did while the chain's buffer was on its way: lanes of
    (worker index, thread, name, [(start, duration, span name index, category, pipeline, key, tuples, part of the chain)],
    [(start, duration, CPU state, waking thread)]) in us relative to the chain's start. Gaps on worker threads are untraced work; the CPU states need scripts/latency_sched.sh."""
    start, end = chain["segments"][0][1], chain["segments"][-1][2]
    pad = max(500_000, (end - start) // 10)
    low, high = start - pad, end + pad
    by_name = {worker.name: worker for worker in workers}
    lanes = []
    for worker in chain_workers(chain, by_name):
        for thread in sorted(set(worker.spans) | set(worker.cpu)):
            spans = worker.spans.get(thread, [])
            role = thread_role(spans)
            # ponytail: steps back a few spans to catch spans that began before the window; spans of a thread barely overlap.
            first = max(0, bisect.bisect_left(spans, (low,)) - 4)
            segments, covered = [], []
            for t0, t1, name, category, key, pipeline, tuples in spans[first:]:
                if t0 >= high:
                    break
                if t1 <= low:
                    continue
                a, b = max(t0, low), min(t1, high)
                relevant = int((worker.name, key, pipeline) in chain["tasks"])
                segments.append([round((a - start) / 1000), round((b - a) / 1000), span_names.setdefault(name, len(span_names)), category,
                                 pipeline, "/".join(map(str, key)) if key != (0, 0, 0) else "", tuples, relevant])
                covered.append((a, b))
            if role == "worker":
                position = low
                for a, b in sorted(covered) + [(high, high)]:
                    if a > position:
                        segments.append([round((position - start) / 1000), round((a - position) / 1000),
                                         span_names.setdefault(UNTRACED_WORK, len(span_names)), UNTRACED, 0, "", 0, 0])
                    position = max(position, b)
            cpu = worker.cpu.get(thread, [])
            first = max(0, bisect.bisect_left(cpu, (low,)) - 1)
            cpu_segments = [[round((max(t0, low) - start) / 1000), round((min(t1, high) - max(t0, low)) / 1000), state, waker]
                            for t0, t1, state, waker in cpu[first:] if t1 > low and t0 < high]
            if segments or cpu_segments:
                name = worker.thread_ids.get(thread, (0, ""))[1] or f"{role} thread {thread}"
                lanes.append([workers.index(worker), thread, name, segments, cpu_segments])
    return {"from": round((low - start) / 1000), "to": round((high - start) / 1000), "lanes": lanes}


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
    traced = {chain["query"] for chain in chains}
    by_total = sorted(range(len(chains)), key=lambda i: total(chains[i]))
    rest = by_total[:-ACTIVITY_SLOWEST]
    detail = set(by_total[-ACTIVITY_SLOWEST:]) | set(rest[:: max(1, len(rest) // ACTIVITY_SAMPLED)])
    span_names = {}
    activity = {i: thread_activity(chains[i], workers, span_names) for i in sorted(detail)}
    data = {"stages": list(stages), "nodes": list(nodes), "queries": list(queries), "chains": rows, "graph": graph(workers, {chain["query"] for chain in chains}),
            "workers": [worker.name for worker in workers],
            "pipelineLabels": [{pipeline: f"{worker.label(pipeline)} · {worker.queries[pipeline]}" for pipeline in worker.pipelines
                                if worker.queries[pipeline] in traced} for worker in workers], "spanNames": list(span_names), "activity": activity, "cpuStates": CPU_STATES,
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
