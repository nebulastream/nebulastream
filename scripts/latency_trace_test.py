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

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import latency_trace as lt  # noqa: E402

US = 1000


def write(directory, pid, steady, system, plan, events):
    path = Path(directory) / f"t.{pid}.bin"
    with path.open("wb") as f:
        f.write(lt.HEADER.pack(b"NESLAT02", pid, steady, system, b"host"))
        for e in events:
            f.write(lt.EVENT.pack(*e))
    path.with_suffix(".plan").write_text(plan)


def begin(stage, t, thread, key=(0, 0, 0), pipeline=0):
    return (t, 0, key[0], key[1], 0, 0, key[2], 0, pipeline, stage, lt.BEGIN, thread)


def end(stage, t, thread, key=(0, 0, 0), pipeline=0, extra=0):
    return (t, 0, key[0], key[1], 0, extra, key[2], 0, pipeline, stage, lt.END, thread)


def span(stage, key, t0, t1, pipeline, thread, extra=0):
    return [begin(stage, t0, thread, key, pipeline), end(stage, t1, thread, key, pipeline, extra)]


def point(stage, key, t, pipeline=0, key2=(0, 0, 0)):
    return (t, 0, key[0], key[1], key2[0], key2[1], key[2], key2[2], pipeline, stage, lt.POINT, 0)


SOURCE, ENGINE = 0, 1
raw, formatted, window = (1, 1, 1), (1, 1, 2), (9, 1, 1)
with tempfile.TemporaryDirectory() as d:
    # Worker A: data arrives at 100us, the source blocks on input until then.
    write(d, 1, 0, 0, "query q(distributed=Q1)\nsource 1 1 GeneratorSource\npipeline 1 2 InputFormatter\npipeline 2 - NetworkSink(connectionId: b:9090, channelId: c1)\n"
          # A query that was never traced, e.g. because tracing is turned on at runtime only for Q1.
          "query u(distributed=Untraced)\nsource 7 7 GeneratorSource\npipeline 7 - PrintSink\ncounter 1 task_queue_admission\n", [
        (5 * US, 0, 0, 0, 0, 3, 0, 0, 1, lt.COUNTER, lt.POINT, 7),
        (9 * US, 0, 0, 0, 0, 2**64 - 1, 0, 0, 1, lt.COUNTER, lt.POINT, 7),
        # The source spans only learn the key when the fill ends.
        begin(lt.SOURCE_BACKPRESSURE, 0, SOURCE), end(lt.SOURCE_BACKPRESSURE, 10 * US, SOURCE),
        begin(lt.SOURCE_BUFFER_WAIT, 10 * US, SOURCE), end(lt.SOURCE_BUFFER_WAIT, 20 * US, SOURCE),
        begin(lt.SOURCE_FILL, 20 * US, SOURCE), end(lt.SOURCE_FILL, 300 * US, SOURCE, raw, 1, 100 * US),
        *span(lt.ENQUEUE, raw, 310 * US, 320 * US, 1, SOURCE),
        # The formatter enqueues its output while it executes, so the enqueue nests inside the execution.
        begin(lt.EXEC, 330 * US, ENGINE, raw, 1),
        point(lt.EMIT, formatted, 350 * US, 1, raw),
        *span(lt.ENQUEUE, formatted, 351 * US, 352 * US, 2, ENGINE),
        end(lt.EXEC, 360 * US, ENGINE, raw, 1),
        *span(lt.EXEC, formatted, 400 * US, 410 * US, 2, ENGINE),
        # Waits for the next task belong to no buffer; the worker still waits when the trace ends.
        *span(lt.TASK_QUEUE_WAIT, (0, 0, 0), 365 * US, 395 * US, 0, ENGINE),
        begin(lt.TASK_QUEUE_WAIT, 450 * US, ENGINE),
        point(lt.NET_SEND, formatted, 500 * US),
    ])
    # Worker B: its steady clock is 1000us ahead of the system clock.
    write(d, 2, 2000 * US, 1000 * US, "query q2(distributed=Q1)\nsource 1 3 SourceThread(implementation:NetworkSource(c1))\npipeline 3 4 WindowBuild\npipeline 4 5 Probe\npipeline 5 - PrintSink\n", [
        point(lt.NET_RECV, formatted, 1600 * US, 3),
        # A source of another query with the same originId must not shadow the events above.
        *span(lt.SOURCE_FILL, formatted, 1000 * US, 1010 * US, 99, 5),
        *span(lt.ENQUEUE, formatted, 1620 * US, 1630 * US, 3, SOURCE),
        begin(lt.EXEC, 1650 * US, ENGINE, formatted, 3),
        point(lt.EMIT, window, 1660 * US, 3, formatted),
        *span(lt.ENQUEUE, window, 1661 * US, 1661 * US, 4, ENGINE),
        end(lt.EXEC, 1670 * US, ENGINE, formatted, 3),
        begin(lt.EXEC, 1680 * US, ENGINE, window, 4),
        point(lt.EMIT, window, 1700 * US, 4, window),
        *span(lt.ENQUEUE, window, 1701 * US, 1701 * US, 5, ENGINE),
        end(lt.EXEC, 1710 * US, ENGINE, window, 4),
        *span(lt.EXEC, window, 1750 * US, 1800 * US, 5, ENGINE),
        # Tracing was turned on while this buffer was in flight, so only its last execution was recorded.
        *span(lt.ENQUEUE, (5, 5, 5), 1760 * US, 1761 * US, 5, 2),
        *span(lt.EXEC, (5, 5, 5), 1770 * US, 1780 * US, 5, 2),
        # A probe that is still running when the trace ends, and an end whose begin was not recorded.
        begin(lt.EXEC, 1790 * US, 3, (9, 2, 1), 4),
        end(lt.EXEC, 1795 * US, 4, (9, 3, 1), 4),
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
