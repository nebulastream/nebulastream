# Watermark gap benchmark (PR #2145 review)

This is not meant for merge. It reproduces the measurements from the review of #2145. The branch is the PR head plus two commits:

1. **A local counter patch**, enabled only when its environment variables are set:
   - `NES_BPSTAT_FILE` gets one line per throttle hold or release, sink hard vote, and source block or unblock.
   - `NES_BPSTAT_POOL_FILE` gets the global buffer pool occupancy every 100 ms.
2. **This harness.**

## What it does

`bench.py` runs one systest query per scenario and arm (`max_watermark_gap_in_ms`), and feeds it from Python producers:

- **TCP:** the producer is a server, and the NES TCP source connects to it.
- **MQTT:** the producer publishes with QoS 0 to a mosquitto broker. The harness starts the broker as a docker container in the dev container's network namespace, so it needs the docker CLI and socket inside the container and the image `eclipse-mosquitto:2`.
- **File:** generated once into `--data-dir`.

Record `n` of a source has event time `T0 + start + n * 1000 / rate` and key `(n * keymul) % keys`. A scenario only changes when a producer may send which records, never the records themselves. So every scenario over the same data must produce the same output checksum, in every arm. `summarize.py` reports a run as `DIFF` when its checksum differs from the gap 0 run of the same scenario.

Each run writes `<out>/<scenario>__g<gap>/` containing:
- `summary.json`;
- `bpstat.log` and `pool.log`;
- `prod_<source>.csv`: the producer schedule, data sent, and time blocked in `send`;
- `tcpq.csv`: socket queue sizes;
- `arrivals.csv`: when each window reached the sink;
- `nes.csv`: RSS and CPU;
- `systest.log`.

The metrics:

- **Pool %:** the peak share of the global buffer pool in use. The join state lives there, and the query fails once the pool runs out.
- **`exc50` / `exc99` / `excMax`:** how late each window reaches the sink, measured from the moment every producer had sent data past the window's end. This is the latency the throttle could add.
- **`holds` / `blocked s`:** holds cast by the throttle, and seconds each source spent blocked.
- **`prodLag` / `sendBlk`:** how far a producer fell behind its schedule, and how long it was blocked in `send`. This is where held data ends up for TCP.

## Running

Inside the dev container, with a release build of `systest`:

```
export NES_SYSTEST=<build-dir>/nes-systests/systest/systest   # default: <repo>/cmake-build-release/...
python3 scripts/benchmarking/watermark-gap/bench.py --out /tmp/bp/low --rate 1000 --keys 10000 --scenarios delay20s file_live
python3 scripts/benchmarking/watermark-gap/summarize.py /tmp/bp/low/results.jsonl
```

Each run takes the scenario's event-time span in real time: 60 s by default, plus up to 30 s of delay. `matrix.sh <out-dir> [batch...]` runs every batch from the review, which takes about 3 h.

| Batch | Load | What it covers |
|---|---|---|
| `low` | 1k rec/s per source, 10k keys, 256 MB pool | Two-way join, 13 delivery patterns |
| `low_q` | Same | Join over a join (`j3_*`), aggregation over a union of 8 sources (`u8_*`) |
| `high` | 20k rec/s, 400k keys, 4 GB pool | Held data overflows the socket buffers and producers block |
| `twonode` | Same as `low` | Two-node topology; the join's inputs arrive over the network channel |
| `mqtt_high` | Same as `high` | MQTT |
| `low_inflight8` | Same as `low`, `default_max_inflight_buffers=8` | The in-flight overshoot |

## The review's findings, one command each

- **Pool exhaustion without the throttle:** `--rate 1000 --keys 10000 --scenarios file_live`. At gap 0, A is read from a file at full speed and joined with live TCP, and the pool runs out.
- **The bound is the gap plus the event time in flight:** `--rate 1000 --keys 10000 --gaps 1000 --scenarios delay20s`, and the same with `--worker-arg=--worker.default_max_inflight_buffers=8`. In `bpstat.log`, compare each `hold` line with the next `release` line of the same origin. Both lines give the origin's watermark first and the slowest watermark second.
- **Held data in the paho queue:** `--transport mqtt --rate 20000 --keys 400000 --worker-arg=--worker.total_memory_in_bytes=4294967296 --gaps 1000 --scenarios mqtt_delay20s`.
- **Two held inputs in stacked joins:** `--rate 1000 --keys 10000 --scenarios j3_adelay20s`. The pool runs out at gap 0 and at 10 s, but not at 1 s.
