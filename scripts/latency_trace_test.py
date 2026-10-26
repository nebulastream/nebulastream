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

"""Synthetic two-worker trace: source -> formatter -> network sink | network source -> window build -> probe -> sink.

    uv run scripts/latency_trace_test.py
"""

import struct
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import latency_trace as lt  # noqa: E402

US = 1000


# The event types the test writes, as the C++ ring describes them: id, category and fields in payload order.
TYPES = {
    "source backpressure": (0, 1, []),
    "source buffer wait": (1, 1, []),
    "source fill": (2, 2, [("key", "k"), ("pipeline", "u"), ("firstData", "u")]),
    "enqueue": (3, 1, [("key", "k"), ("pipeline", "u")]),
    "exec": (4, 0, [("key", "k"), ("pipeline", "u"), ("tuples", "u")]),
    "task queue wait": (5, 1, []),
    "emit": (6, 2, [("output", "k"), ("input", "k"), ("pipeline", "u")]),
    "net send": (7, 2, [("key", "k")]),
    "net recv": (8, 2, [("key", "k"), ("pipeline", "u"), ("received", "u")]),
    "counter": (9, 2, [("counter", "u"), ("value", "i")]),
}
SOURCE_BACKPRESSURE, SOURCE_BUFFER_WAIT, SOURCE_FILL = "source backpressure", "source buffer wait", "source fill"
ENQUEUE, EXEC, EMIT, NET_SEND, NET_RECV = "enqueue", "exec", "emit", "net send", "net recv"
TASK_QUEUE_WAIT = "task queue wait"


def layout(fields):
    return struct.Struct("<" + "".join(lt.FIELD_FORMATS[kind] for _, kind in fields))


def name(text):
    return struct.pack("<H", len(text)) + text.encode()


def schemas():
    out = struct.pack("<H", len(TYPES))
    for type_name, (type_id, category, fields) in TYPES.items():
        out += struct.pack("<H", type_id) + name(type_name) + struct.pack("<BIH", category, layout(fields).size, len(fields))
        offset = 0
        for field, kind in fields:
            out += name(field) + struct.pack("<cI", kind.encode(), offset)
            offset += struct.calcsize("<" + lt.FIELD_FORMATS[kind])
    return out


def write(directory, pid, steady, system, plan, events):
    """Writes each event as its own chunk; `events` are (thread, kind, time, type name, field values)."""
    path = Path(directory) / f"t.{pid}.bin"
    with path.open("wb") as f:
        f.write(lt.HEADER.pack(b"NESLAT03", pid, steady, system, b"host") + schemas())
        for thread, kind, time, type_name, values in events:
            type_id, _, fields = TYPES[type_name]
            flat = [v for field, k in fields for v in (values.get(field, (0, 0, 0)) if k == "k" else (values.get(field, 0),))]
            record = lt.RECORD.pack(type_id, kind, time) + layout(fields).pack(*flat)
            f.write(lt.CHUNK.pack(thread, len(record)) + record)
    path.with_suffix(".plan").write_text(plan)


def fields_of(stage, key, pipeline, extra):
    extra_field = {SOURCE_FILL: "firstData", EXEC: "tuples"}.get(stage)
    return {"key": key, "pipeline": pipeline, **({extra_field: extra} if extra_field else {})}


def begin(stage, t, thread, key=(0, 0, 0), pipeline=0):
    return (thread, lt.BEGIN, t, stage, fields_of(stage, key, pipeline, 0))


def end(stage, t, thread, key=(0, 0, 0), pipeline=0, extra=0):
    return (thread, lt.END, t, stage, fields_of(stage, key, pipeline, extra))


def span(stage, key, t0, t1, pipeline, thread, extra=0):
    return [begin(stage, t0, thread, key, pipeline), end(stage, t1, thread, key, pipeline, extra)]


def point(stage, key, t, pipeline=0, key2=(0, 0, 0)):
    if stage == EMIT:
        return (0, lt.INSTANT, t, stage, {"output": key, "input": key2, "pipeline": pipeline})
    return (0, lt.INSTANT, t, stage, {"key": key, "pipeline": pipeline, "received": t})


def counter(t, identifier, value):
    return (7, lt.INSTANT, t, "counter", {"counter": identifier, "value": value})


SOURCE, ENGINE = 0, 1
raw, formatted, window = (1, 1, 1), (1, 1, 2), (9, 1, 1)
with tempfile.TemporaryDirectory() as d:
    # Worker A: data arrives at 100us, the source blocks on input until then.
    write(d, 1, 0, 0, "query q(distributed=Q1)\nsource 1 1 GeneratorSource\npipeline 1 2 InputFormatter\npipeline 2 - NetworkSink(connectionId: b:9090, channelId: c1)\n"
          # A query that was never traced, e.g. because tracing is turned on at runtime only for Q1.
          "query u(distributed=Untraced)\nsource 7 7 GeneratorSource\npipeline 7 - PrintSink\ncounter 1 task_queue_admission\n", [
        counter(5 * US, 1, 3),
        counter(9 * US, 1, -1),
        # The source spans only learn the key when the fill ends.
        begin(SOURCE_BACKPRESSURE, 0, SOURCE), end(SOURCE_BACKPRESSURE, 10 * US, SOURCE),
        begin(SOURCE_BUFFER_WAIT, 10 * US, SOURCE), end(SOURCE_BUFFER_WAIT, 20 * US, SOURCE),
        begin(SOURCE_FILL, 20 * US, SOURCE), end(SOURCE_FILL, 300 * US, SOURCE, raw, 1, 100 * US),
        *span(ENQUEUE, raw, 310 * US, 320 * US, 1, SOURCE),
        # The formatter enqueues its output while it executes, so the enqueue nests inside the execution.
        begin(EXEC, 330 * US, ENGINE, raw, 1),
        point(EMIT, formatted, 350 * US, 1, raw),
        *span(ENQUEUE, formatted, 351 * US, 352 * US, 2, ENGINE),
        end(EXEC, 360 * US, ENGINE, raw, 1),
        *span(EXEC, formatted, 400 * US, 410 * US, 2, ENGINE),
        # Waits for the next task belong to no buffer; the worker still waits when the trace ends.
        *span(TASK_QUEUE_WAIT, (0, 0, 0), 365 * US, 395 * US, 0, ENGINE),
        begin(TASK_QUEUE_WAIT, 450 * US, ENGINE),
        point(NET_SEND, formatted, 500 * US),
    ])
    # Worker B: its steady clock is 1000us ahead of the system clock.
    write(d, 2, 2000 * US, 1000 * US, "query q2(distributed=Q1)\nsource 1 3 SourceThread(implementation:NetworkSource(c1))\npipeline 3 4 WindowBuild\npipeline 4 5 Probe\npipeline 5 - PrintSink\n", [
        point(NET_RECV, formatted, 1600 * US, 3),
        # A source of another query with the same originId must not shadow the events above.
        *span(SOURCE_FILL, formatted, 1000 * US, 1010 * US, 99, 5),
        *span(ENQUEUE, formatted, 1620 * US, 1630 * US, 3, SOURCE),
        begin(EXEC, 1650 * US, ENGINE, formatted, 3),
        point(EMIT, window, 1660 * US, 3, formatted),
        *span(ENQUEUE, window, 1661 * US, 1661 * US, 4, ENGINE),
        end(EXEC, 1670 * US, ENGINE, formatted, 3),
        begin(EXEC, 1680 * US, ENGINE, window, 4),
        point(EMIT, window, 1700 * US, 4, window),
        *span(ENQUEUE, window, 1701 * US, 1701 * US, 5, ENGINE),
        end(EXEC, 1710 * US, ENGINE, window, 4),
        *span(EXEC, window, 1750 * US, 1800 * US, 5, ENGINE),
        # Tracing was turned on while this buffer was in flight, so only its last execution was recorded.
        *span(ENQUEUE, (5, 5, 5), 1760 * US, 1761 * US, 5, 2),
        *span(EXEC, (5, 5, 5), 1770 * US, 1780 * US, 5, 2),
        # A probe that is still running when the trace ends, and an end whose begin was not recorded.
        begin(EXEC, 1790 * US, 3, (9, 2, 1), 4),
        end(EXEC, 1795 * US, 4, (9, 3, 1), 4),
    ])
    workers, sends = lt.load(sorted(Path(d).glob("*.bin")))
    # Every pipeline runs once here, so the only chain starts during warm-up.
    assert lt.stitch(workers, sends)[0] == []
    chains, incomplete, _ = lt.stitch(workers, sends, skip_warmup=False)
    lt.interactive_report(workers, chains, Path(d))
    page = (Path(d) / "report.html").read_text()
    assert "/*TRACE_DATA*/" not in page and '"network":true' in page, "report embeds the data and the network edge"
    assert "Untraced" not in page, "the graph only shows traced queries"
    assert workers[0].sources[raw, 1][lt.SOURCE_BACKPRESSURE] == (0, 10 * US, 0), "keyless waits belong to the next fill"
    assert lt.counter_series(workers) == [("task_queue_admission", "host:1", [(5 * US, 3), (9 * US, -1), (500 * US, -1)])]
    activity = lt.thread_activity(chains[0], workers, names := {})
    lanes = {(worker, thread): (role, [(list(names)[n], start, us) for start, us, n, _ in segments])
             for worker, thread, role, segments in activity["lanes"]}
    role, segments = lanes[0, ENGINE]
    assert role == "worker" and ("task queue wait", 365 - 100, 30) in segments, segments
    assert ("untraced work", 410 - 100, 40) in segments, "gaps between spans of a worker thread are untraced work"
    spans = lt.open_spans(workers)
    assert [(stage, key) for stage, _, _, key, _ in spans] == [("exec", "9/2/1")], spans
    assert spans[0][4] == (1800 - 1790) / 1000, "open for the time until the last event"
    assert sum(w.unpaired for w in workers) == 1

assert incomplete == 1 and len(chains) == 1, (chains, incomplete)
segments = chains[0]["segments"]
names = [name for name, _, _ in segments]
assert names[0] == "source fill (batching)", names
assert chains[0]["query"] == "Q1"
assert "network" in names and "enqueue P1 InputFormatter" in names, names
assert segments[0][1] == 100 * US, "latency starts at first data"
assert segments[-1][2] == 800 * US, "sink exec end on worker B in system time"
assert all(a[2] == b[1] for a, b in zip(segments, segments[1:])), "segments are contiguous"
assert sum(t1 - t0 for _, t0, t1 in segments) == 700 * US
print("ok:", *(f"{n} {(t1 - t0) / US:.0f}us" for n, t0, t1 in segments), sep="\n  ")
