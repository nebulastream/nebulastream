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
        f.write(lt.HEADER.pack(b"NESLAT01", pid, steady, system, b"host"))
        for e in events:
            f.write(lt.EVENT.pack(*e))
    path.with_suffix(".plan").write_text(plan)


def ev(stage, key, t0, t1=0, pipeline=0, key2=(0, 0, 0)):
    return (t0, t1, key[0], key[1], key2[0], key2[1], key[2], key2[2], pipeline, stage, 0)


raw, formatted, window = (1, 1, 1), (1, 1, 2), (9, 1, 1)
with tempfile.TemporaryDirectory() as d:
    # Worker A: data arrives at 100us, the source blocks on input until then.
    write(d, 1, 0, 0, "query q(distributed=Q1)\nsource 1 1 GeneratorSource\npipeline 1 2 InputFormatter\npipeline 2 - NetworkSink(connectionId: b:9090, channelId: c1)\n"
          # A query that was never traced, e.g. because tracing is turned on at runtime only for Q1.
          "query u(distributed=Untraced)\nsource 7 7 GeneratorSource\npipeline 7 - PrintSink\n", [
        ev(lt.SOURCE_BACKPRESSURE, raw, 0, 10 * US, 1),
        ev(lt.SOURCE_BUFFER_WAIT, raw, 10 * US, 20 * US, 1),
        (20 * US, 300 * US, *raw[:2], 0, 100 * US, raw[2], 0, 1, lt.SOURCE_FILL, 0),
        ev(lt.ENQUEUE, raw, 310 * US, 320 * US, 1),
        ev(lt.EMIT, formatted, 350 * US, 0, 1, raw),
        ev(lt.EXEC, raw, 330 * US, 360 * US, 1),
        ev(lt.ENQUEUE, formatted, 351 * US, 352 * US, 2),
        ev(lt.EXEC, formatted, 400 * US, 410 * US, 2),
        ev(lt.NET_SEND, formatted, 500 * US),
    ])
    # Worker B: its steady clock is 1000us ahead of the system clock.
    write(d, 2, 2000 * US, 1000 * US, "query q2(distributed=Q1)\nsource 1 3 SourceThread(implementation:NetworkSource(c1))\npipeline 3 4 WindowBuild\npipeline 4 5 Probe\npipeline 5 - PrintSink\n", [
        ev(lt.NET_RECV, formatted, 1600 * US, 0, 3),
        # A source of another query with the same originId must not shadow the events above.
        ev(lt.SOURCE_FILL, formatted, 1000 * US, 1010 * US, 99),
        ev(lt.ENQUEUE, formatted, 1620 * US, 1630 * US, 3),
        ev(lt.EMIT, window, 1660 * US, 0, 3, formatted),
        ev(lt.EXEC, formatted, 1650 * US, 1670 * US, 3),
        ev(lt.ENQUEUE, window, 1661 * US, 1661 * US, 4),
        ev(lt.EMIT, window, 1700 * US, 0, 4, window),
        ev(lt.EXEC, window, 1680 * US, 1710 * US, 4),
        ev(lt.ENQUEUE, window, 1701 * US, 1701 * US, 5),
        ev(lt.EXEC, window, 1750 * US, 1800 * US, 5),
        # Tracing was turned on while this buffer was in flight, so only its last execution was recorded.
        ev(lt.ENQUEUE, (5, 5, 5), 1760 * US, 1761 * US, 5),
        ev(lt.EXEC, (5, 5, 5), 1770 * US, 1780 * US, 5),
    ])
    workers, sends = lt.load(sorted(Path(d).glob("*.bin")))
    # Every pipeline runs once here, so the only chain starts during warm-up.
    assert lt.stitch(workers, sends)[0] == []
    chains, incomplete, _ = lt.stitch(workers, sends, skip_warmup=False)
    lt.interactive_report(workers, chains, Path(d))
    page = (Path(d) / "report.html").read_text()
    assert "/*TRACE_DATA*/" not in page and '"network":true' in page, "report embeds the data and the network edge"
    assert "Untraced" not in page, "the graph only shows traced queries"

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
