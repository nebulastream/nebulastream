#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Benchmark harness for the watermark gap throttle of PR #2145.

Runs inside the dev container. One run = one scenario under one arm (max_watermark_gap_in_ms).
Producers serve CSV over TCP (NES connects as client) or publish to MQTT. Every source's data
is a pure function of the record index, so all delivery schedules of the same data must give
the same query result. A scenario only changes *when* each producer may send which data:
avail(s) is the event time (seconds after T0) that the producer may have sent by wall offset s.

Outputs per run (in <out>/<run>/): summary.json, nes.csv (rss/cpu), prod_<src>.csv,
arrivals.csv (sink rows: wall arrival, window end), bpstat.log, systest.log.
"""
import argparse
import json
import math
import multiprocessing as mp
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import zlib

REPO = os.environ.get("NES_REPO", os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../..")))
SYSTEST = os.environ.get("NES_SYSTEST", f"{REPO}/cmake-build-release/nes-systests/systest/systest")
T0_MS = 1_000_000_000_000  # event time origin; fixed so results are reproducible
TCP_BASE_PORT = 19100
TOPOLOGY = f"{REPO}/nes-systests/configs/topologies/single-node.yaml"
EXTRA_WORKER_ARGS = []
KEEP_WD = os.environ.get("BP_KEEP_WD") == "1"

# ----------------------------------------------------------------------------------------------
# Data: record n of a source has event time start_ms + n*1000/rate, key (n*keymul) % keys.


def record_ts_ms(src, n):
    return src["start_ms"] + (n * 1000) // src["rate"]


def total_records(src):
    return ((src["end_ms"] - src["start_ms"]) * src["rate"]) // 1000


def gen_chunk(src, i, j):
    rate, start, keys, mul = src["rate"], src["start_ms"], src["keys"], src["keymul"]
    gaps = src.get("gaps_ms", [])
    lines = []
    for n in range(i, j):
        ts = start + (n * 1000) // rate
        if gaps and any(a <= ts < b for a, b in gaps):
            continue
        k = (n * mul) % keys
        lines.append(f"{T0_MS + ts},{k},{n},{n},{n},{n}")
    return lines


def first_record_after(src, et_ms):
    """Number of records with event time <= et_ms."""
    if et_ms < src["start_ms"]:
        return 0
    n = ((et_ms - src["start_ms"]) * src["rate"]) // 1000 + 1
    return min(n, total_records(src))


# ----------------------------------------------------------------------------------------------
# Delivery schedules: avail(s) -> event time in ms (relative to T0) the producer may have sent.


def make_avail(spec):
    kind = spec.get("kind", "live")
    if kind == "live":
        d = spec.get("delay_ms", 0)
        return lambda s: s * 1000 - d
    if kind == "sine":
        amp, period, phase = spec["amp_ms"], spec["period_s"], spec.get("phase", 0.0)
        return lambda s: s * 1000 - amp / 2 * (1 + math.sin(2 * math.pi * s / period + phase))
    if kind == "jitter":
        rng = random.Random(spec.get("seed", 1))
        slot = spec["slot_s"]
        delays = [rng.uniform(0, spec["max_ms"]) for _ in range(10000)]
        return lambda s: s * 1000 - delays[int(s / slot)]
    if kind == "batch":
        every = spec["every_s"]
        return lambda s: (s - (s % every)) * 1000
    if kind == "outage":
        t1, t2 = spec["from_s"], spec["to_s"]
        return lambda s: (s if s < t1 else (t1 if s < t2 else s)) * 1000
    if kind == "unbounded":  # all data available immediately (send as fast as possible)
        return lambda s: float("inf")
    raise ValueError(kind)


# ----------------------------------------------------------------------------------------------
# Producers


def mqtt_varint(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def mqtt_connect(host, port, client_id):
    sock = socket.create_connection((host, port))
    cid = client_id.encode()
    body = b"\x00\x04MQTT\x04\x02\x00\x00" + len(cid).to_bytes(2, "big") + cid
    sock.sendall(b"\x10" + mqtt_varint(len(body)) + body)
    ack = sock.recv(4)
    assert ack[:1] == b"\x20" and ack[3:4] == b"\x00", ack
    return sock


def mqtt_publish(sock, topic, payload):
    t = topic.encode()
    body = len(t).to_bytes(2, "big") + t + payload
    sock.sendall(b"\x30" + mqtt_varint(len(body)) + body)


def producer(src, out_dir, ready, go, wall_start):
    """One process per source. Waits for its connection, then follows the schedule."""
    avail = make_avail(src["schedule"])
    transport = src["transport"]
    if transport == "tcp":
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", src["port"]))
        srv.listen(1)
        ready.set()
        conn, _ = srv.accept()
        send = lambda lines: conn.sendall(("\n".join(lines) + "\n").encode())
    elif transport == "mqtt":
        conn = mqtt_connect("127.0.0.1", 1883, f"bp-prod-{src['name']}")
        ready.set()
        topic = src["topic"]
        # NES appends a newline to every message.
        send = lambda lines: mqtt_publish(conn, topic, "\n".join(lines).encode())
    else:
        raise ValueError(transport)
    go.wait()
    start = wall_start.value
    total = total_records(src)
    chunk = src.get("chunk", 2000)
    sent = 0
    sent_bytes = 0
    blocked = 0.0
    samples = []
    last_sample = -1.0
    while sent < total:
        now = time.time()
        s = now - start
        target = first_record_after(src, int(avail(s))) if avail(s) != float("inf") else total
        if target > sent:
            upto = min(target, sent + chunk)
            lines = gen_chunk(src, sent, upto)
            t = time.time()
            if lines:
                send(lines)
            blocked += time.time() - t
            sent_bytes += sum(len(x) + 1 for x in lines)
            sent = upto
        else:
            time.sleep(0.002)
        if s - last_sample >= 0.05:
            last_sample = s
            a = avail(s)
            sent_et = record_ts_ms(src, sent - 1) if sent else src["start_ms"]
            samples.append((round(s, 3), a if a != float("inf") else -1, sent_et, sent_bytes, round(blocked, 3)))
    s = time.time() - start
    samples.append((round(s, 3), -1, src["end_ms"], sent_bytes, round(blocked, 3)))
    if transport == "tcp":
        conn.shutdown(socket.SHUT_WR)
        # Wait for NES to close its end, so EoS reaches the source before we exit.
        conn.settimeout(600)
        try:
            while conn.recv(4096):
                pass
        except OSError:
            pass
        conn.close()
        srv.close()
    else:
        conn.close()
    with open(os.path.join(out_dir, f"prod_{src['name']}.csv"), "w") as f:
        f.write("s,avail_ms,sent_ms,bytes,blocked_s\n")
        for row in samples:
            f.write(",".join(str(x) for x in row) + "\n")


# ----------------------------------------------------------------------------------------------
# Test file generation

FIELDS = ["ts", "key", "val", "p1", "p2", "p3"]


def schema(name):
    return ", ".join(f"{name}_{f} UINT64 NOT NULL" for f in FIELDS)


def source_ddl(src):
    n = src["name"]
    ddl = f"CREATE LOGICAL SOURCE {n}({schema(n)});\n"
    flush = src.get("flush_ms", 10)
    if src["transport"] == "tcp":
        ddl += (
            f"CREATE PHYSICAL SOURCE FOR {n} TYPE TCP SET('127.0.0.1' AS \"SOURCE\".SOCKET_HOST, "
            f"{src['port']} AS \"SOURCE\".SOCKET_PORT, {flush} AS \"SOURCE\".FLUSH_INTERVAL_MS);\n"
        )
    elif src["transport"] == "mqtt":
        ddl += (
            f"CREATE PHYSICAL SOURCE FOR {n} TYPE MQTT SET('tcp://127.0.0.1:1883' AS \"SOURCE\".SERVER_URI, "
            f"'{src['topic']}' AS \"SOURCE\".TOPIC, {flush} AS \"SOURCE\".FLUSH_INTERVAL_MS, 0 AS \"SOURCE\".QOS);\n"
        )
    elif src["transport"] == "file":
        ddl += f"CREATE PHYSICAL SOURCE FOR {n} TYPE File;\nATTACH FILE {src['file']}\n"
    return ddl


def query_sql(query, srcs):
    names = [s["name"] for s in srcs]
    if query == "join2":
        a, b = names
        sink = "start UINT64 NOT NULL, end UINT64 NOT NULL, k UINT64 NOT NULL, l UINT64 NOT NULL, r UINT64 NOT NULL"
        sql = (
            f"SELECT start, end, {a}_key AS k, {a}_val AS l, {b}_val AS r FROM (SELECT * FROM {a}) "
            f"INNER JOIN (SELECT * FROM {b}) ON {a}_key = {b}_key WINDOW TUMBLING ({a}_ts, {b}_ts, size 1 sec) INTO out;"
        )
    elif query == "join3":
        a, b, c = names
        sink = "start UINT64 NOT NULL, end UINT64 NOT NULL, k UINT64 NOT NULL, l UINT64 NOT NULL, r UINT64 NOT NULL"
        sql = (
            f"SELECT start, end, {a}_key AS k, {a}_val AS l, {c}_val AS r "
            f"FROM (SELECT {a}_ts, {a}_key, {a}_val FROM (SELECT * FROM {a}) INNER JOIN (SELECT * FROM {b}) "
            f"ON {a}_key = {b}_key WINDOW TUMBLING ({a}_ts, {b}_ts, size 1 sec)) "
            f"INNER JOIN (SELECT * FROM {c}) ON {a}_key = {c}_key WINDOW TUMBLING ({a}_ts, {c}_ts, size 1 sec) INTO out;"
        )
    elif query == "unionagg":
        sink = "start UINT64 NOT NULL, end UINT64 NOT NULL, key UINT64 NOT NULL, s UINT64 NOT NULL"
        union = " UNION ".join(f"SELECT {n}_ts AS ts, {n}_key AS key, {n}_val AS val FROM {n}" for n in names)
        sql = f"SELECT start, end, key, SUM(val) AS s FROM ({union}) GROUP BY key WINDOW TUMBLING (ts, size 1 sec) INTO out;"
    else:
        raise ValueError(query)
    return f"CREATE SINK out({sink}) TYPE File;\n\n{sql}\n----\n"


def write_test(path, scenario):
    with open(path, "w") as f:
        f.write(f"# name: bp/{scenario['name']}.test\n# description: generated\n# groups: [BP]\n\n")
        for src in scenario["sources"]:
            f.write(source_ddl(src))
        f.write("\n" + query_sql(scenario["query"], scenario["sources"]))


def write_file_source(src, data_dir):
    path = os.path.join(data_dir, src["file"])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    key = json.dumps({k: src[k] for k in ("rate", "keys", "keymul", "start_ms", "end_ms")}, sort_keys=True)
    stamp = path + ".key"
    if os.path.exists(stamp) and open(stamp).read() == key:
        return
    total = total_records(src)
    with open(path, "w") as f:
        for i in range(0, total, 100000):
            f.write("\n".join(gen_chunk(src, i, min(total, i + 100000))) + "\n")
    open(stamp, "w").write(key)


# ----------------------------------------------------------------------------------------------
# Sampling


def proc_stats(pid):
    rss = hwm = 0
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("VmHWM:"):
                    hwm = int(line.split()[1])
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return None
    return rss, hwm, cpu


def tcp_queues(ports):
    """Bytes queued in the kernel per producer port: (producer send queue, NES receive queue)."""
    out = {p: [0, 0] for p in ports}
    try:
        with open("/proc/net/tcp") as f:
            next(f)
            for line in f:
                parts = line.split()
                lport = int(parts[1].split(":")[1], 16)
                rport = int(parts[2].split(":")[1], 16)
                tx, rx = (int(x, 16) for x in parts[4].split(":"))
                if lport in out and parts[3] == "01":
                    out[lport][0] += tx
                elif rport in out and parts[3] == "01":
                    out[rport][1] += rx
    except OSError:
        pass
    return out


class Tail:
    """Follows the sink file and records the wall time at which each row appears."""

    def __init__(self, wd):
        self.wd = wd
        self.path = None
        self.pos = 0
        self.partial = b""
        self.rows = []  # (wall, end_ms_rel)
        self.count = 0
        self.checksum = 0

    def find(self):
        for root, _, files in os.walk(self.wd):
            for name in files:
                if name.endswith(".csv"):
                    return os.path.join(root, name)
        return None

    def poll(self):
        if self.path is None:
            self.path = self.find()
            if self.path is None:
                return
        try:
            with open(self.path, "rb") as f:
                f.seek(self.pos)
                data = f.read()
        except FileNotFoundError:
            return
        if not data:
            return
        now = time.time()
        self.pos += len(data)
        data = self.partial + data
        lines = data.split(b"\n")
        self.partial = lines.pop()
        last_end = None
        for line in lines:
            if not line or not line[:1].isdigit():
                continue
            parts = line.split(b",")
            end = int(parts[1]) - T0_MS
            self.count += 1
            self.checksum = (self.checksum + zlib.crc32(line)) % (1 << 64)
            if end != last_end:
                self.rows.append((now, end))
                last_end = end


# ----------------------------------------------------------------------------------------------
# Running


def kill_tree(proc):
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run(scenario, gap_ms, out_root, threads, data_dir, timeout_s):
    name = f"{scenario['name']}__g{gap_ms}"
    out_dir = os.path.join(out_root, name)
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir)
    wd = os.path.join(out_dir, "wd")
    test_path = os.path.join(out_dir, "q.test")
    for src in scenario["sources"]:
        if src["transport"] == "file":
            write_file_source(src, data_dir)
    write_test(test_path, scenario)
    json.dump({"scenario": scenario, "gap_ms": gap_ms}, open(os.path.join(out_dir, "spec.json"), "w"), indent=1)

    ctx = mp.get_context("fork")
    go = ctx.Event()
    wall_start = ctx.Value("d", 0.0)
    live = [s for s in scenario["sources"] if s["transport"] != "file"]
    readies = []
    procs = []
    for src in live:
        ready = ctx.Event()
        p = ctx.Process(target=producer, args=(src, out_dir, ready, go, wall_start), daemon=True)
        p.start()
        readies.append(ready)
        procs.append(p)
    for r in readies:
        r.wait(10)

    env = dict(os.environ, NES_BPSTAT_FILE=os.path.join(out_dir, "bpstat.log"), NES_BPSTAT_POOL_FILE=os.path.join(out_dir, "pool.log"))
    cmd = [
        SYSTEST, "-t", test_path, "--workingDir", wd, "-n", "1", "--data", data_dir, "--query-timeout", str(timeout_s),
        "-c", TOPOLOGY,
        "--",
        # A negative gap omits the option, for builds without the throttle (main).
        *([f"--worker.default_query_execution.max_watermark_gap_in_ms={gap_ms}"] if gap_ms >= 0 else []),
        f"--worker.query_engine.number_of_worker_threads={threads}",
        *EXTRA_WORKER_ARGS,
    ]
    log = open(os.path.join(out_dir, "systest.log"), "w")
    t_launch = time.time()
    sysproc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)

    # Start the schedule once NES has connected to every TCP producer (or subscribed to MQTT).
    tcp_ports = [s["port"] for s in live if s["transport"] == "tcp"]
    n_mqtt = sum(1 for s in live if s["transport"] == "mqtt")
    deadline = time.time() + 60
    while time.time() < deadline:
        q = established(tcp_ports, n_mqtt, len(live))
        if q:
            break
        time.sleep(0.01)
    if n_mqtt:
        time.sleep(1.0)  # SUBSCRIBE round trip
    wall_start.value = time.time() + 0.1
    go.set()

    tail = Tail(wd)
    broker_peak = 0
    nes_rows = []
    q_rows = []
    while sysproc.poll() is None:
        st = proc_stats(sysproc.pid)
        now = time.time()
        if st:
            nes_rows.append((round(now - wall_start.value, 3), st[0], st[1], round(st[2], 2)))
        if tcp_ports:
            qs = tcp_queues(tcp_ports)
            q_rows.append((round(now - wall_start.value, 3), *[x for p in tcp_ports for x in qs[p]]))
        tail.poll()
        if BROKER and len(nes_rows) % 20 == 1:
            broker_peak = max(broker_peak, broker_rss_mib(BROKER) or 0)
        if n_mqtt and all(not p.is_alive() for p in procs):
            # MQTT has no end of stream: stop once the last window is out, or after a grace period.
            last = max((e for _, e in tail.rows), default=0)
            done_at = getattr(tail, "done_at", None) or now
            tail.done_at = done_at
            if last >= scenario["end_ms"] - 1000 or now - done_at > 20:
                kill_tree(sysproc)
                break
        if now - t_launch > timeout_s + 30:
            kill_tree(sysproc)
            break
        time.sleep(0.05)
    sysproc.wait()
    tail.poll()
    t_end = time.time()
    for p in procs:
        p.join(5)
        if p.is_alive():
            p.kill()

    with open(os.path.join(out_dir, "nes.csv"), "w") as f:
        f.write("s,rss_kib,hwm_kib,cpu_s\n")
        f.writelines(",".join(map(str, r)) + "\n" for r in nes_rows)
    if tcp_ports:
        with open(os.path.join(out_dir, "tcpq.csv"), "w") as f:
            f.write("s," + ",".join(f"{s['name']}_tx,{s['name']}_rx" for s in live if s["transport"] == "tcp") + "\n")
            f.writelines(",".join(map(str, r)) + "\n" for r in q_rows)
    with open(os.path.join(out_dir, "arrivals.csv"), "w") as f:
        f.write("s,end_ms\n")
        f.writelines(f"{round(w - wall_start.value, 3)},{e}\n" for w, e in tail.rows)

    summary = {
        "run": name,
        "scenario": scenario["name"],
        "query": scenario["query"],
        "gap_ms": gap_ms,
        "exit": sysproc.returncode,
        "wall_s": round(t_end - wall_start.value, 2),
        "rows": tail.count,
        "checksum": tail.checksum,
        "peak_rss_mib": round(max((r[2] for r in nes_rows), default=0) / 1024, 1),
        "cpu_s": nes_rows[-1][3] if nes_rows else None,
        "sink_file": tail.path,
        "broker_peak_mib": broker_peak if BROKER else None,
    }
    summary.update(latency(tail.rows, out_dir, scenario))
    summary.update(bpstats(os.path.join(out_dir, "bpstat.log"), wall_start.value))
    summary.update(producer_stats(out_dir, live))
    summary.update(pool_stats(os.path.join(out_dir, "pool.log")))
    summary["error"] = systest_error(os.path.join(out_dir, "systest.log"))
    json.dump(summary, open(os.path.join(out_dir, "summary.json"), "w"), indent=1)
    if not KEEP_WD:
        shutil.rmtree(wd, ignore_errors=True)
    return summary


def pool_stats(path):
    """Peak buffers in use per pool (pooled buffers are where operator state lives)."""
    peak = {}
    total = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                parts = line.split()
                if len(parts) != 6:
                    continue
                pid, n, avail, unpooled = parts[2], int(parts[3]), int(parts[4]), int(parts[5])
                total[pid] = n
                if avail == 0 and unpooled == 0:
                    continue  # the pool is being torn down
                peak[pid] = max(peak.get(pid, 0), n - avail)
    return {"pool_peak_pct": {k: round(100 * peak[k] / total[k], 1) for k in peak}, "pool_buffers": total}


def systest_error(path):
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return None
    for pattern in ("buffer allocation failure", "timed out", "unexpected error", "Connection lost", "Assertion", "Segmentation"):
        if pattern in text:
            m = re.search(r".*" + re.escape(pattern) + r".*", text)
            return m.group(0).strip()[:300]
    return None


def established(tcp_ports, n_mqtt, n_live):
    want = {p: False for p in tcp_ports}
    mqtt = 0
    with open("/proc/net/tcp") as f:
        next(f)
        for line in f:
            parts = line.split()
            if parts[3] != "01":
                continue
            lport = int(parts[1].split(":")[1], 16)
            if lport in want:
                want[lport] = True
            if lport == 1883:
                mqtt += 1
    # Each MQTT source and each MQTT producer holds one connection to the broker.
    return all(want.values()) and mqtt >= 2 * n_mqtt


def pct(values, q):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(q * len(v)))]


def latency(rows, out_dir, scenario):
    """Window latency: arrival of a window's first row minus when the window ended in the live clock,
    and minus when the last source had delivered data past the window end (excess over the inherent wait)."""
    if not rows:
        return {}
    first = {}
    for w, e in rows:
        first.setdefault(e, w)
    # Relative to wall_start: arrivals.csv already has s; recompute from file for simplicity.
    arr = []
    with open(os.path.join(out_dir, "arrivals.csv")) as f:
        next(f)
        for line in f:
            s, e = line.split(",")
            arr.append((float(s), int(e)))
    first = {}
    for s, e in arr:
        first.setdefault(e, s)
    # When did every live source deliver past event time e? From producer samples.
    delivered = []
    for src in scenario["sources"]:
        if src["transport"] == "file":
            continue
        path = os.path.join(out_dir, f"prod_{src['name']}.csv")
        if not os.path.exists(path):
            continue
        pts = []
        with open(path) as f:
            next(f)
            for line in f:
                s, _, sent, _, _ = line.split(",")
                pts.append((float(s), int(sent)))
        delivered.append(pts)

    def ready_at(e):
        t = 0.0
        for pts in delivered:
            hit = next((s for s, sent in pts if sent > e), None)
            if hit is None:
                return None
            t = max(t, hit)
        return t

    lat, excess = [], []
    for e, s in first.items():
        if e > scenario["end_ms"]:
            continue
        lat.append(s - e / 1000)
        r = ready_at(e)
        if r is not None:
            excess.append(s - r)
    if not lat:
        return {}
    return {
        "windows": len(first),
        "lat_p50_s": round(pct(lat, 0.5), 3),
        "lat_p99_s": round(pct(lat, 0.99), 3),
        "lat_max_s": round(max(lat), 3),
        "excess_p50_s": round(pct(excess, 0.5), 3) if excess else None,
        "excess_p99_s": round(pct(excess, 0.99), 3) if excess else None,
        "excess_max_s": round(max(excess), 3) if excess else None,
    }


def bpstats(path, start):
    out = {"holds": 0, "releases": 0, "hard_applies": 0, "blocked_s": {}, "block_episodes": {}}
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            parts = line.split()
            if len(parts) < 3:
                continue
            kind = parts[1]
            if kind == "hold":
                out["holds"] += 1
            elif kind == "release":
                out["releases"] += 1
            elif kind == "hard_apply":
                out["hard_applies"] += 1
            elif kind == "unblock":
                src = parts[2]
                out["blocked_s"][src] = round(out["blocked_s"].get(src, 0) + int(parts[3]) / 1e6, 3)
                out["block_episodes"][src] = out["block_episodes"].get(src, 0) + 1
    return out


def producer_stats(out_dir, live):
    """Producer lag behind its schedule that persisted for at least 0.5 s (data waiting upstream),
    and the total time the producer spent blocked in send."""
    lag = {}
    blocked = {}
    for src in live:
        path = os.path.join(out_dir, f"prod_{src['name']}.csv")
        if not os.path.exists(path):
            continue
        pts = []
        last_blocked = 0.0
        with open(path) as f:
            next(f)
            for line in f:
                s, a, sent, _, b = line.split(",")
                a = float(a)
                last_blocked = float(b)
                if a >= 0:
                    pts.append((float(s), max(0.0, min(a, src["end_ms"]) - int(sent))))
        worst = 0.0
        j = 0
        for i, (t, _) in enumerate(pts):
            while j < len(pts) and pts[j][0] < t + 0.5:
                j += 1
            if j < len(pts) or pts[-1][0] >= t + 0.5:
                worst = max(worst, min(v for _, v in pts[i:j]))
        lag[src["name"]] = round(worst / 1000, 2)
        blocked[src["name"]] = last_blocked
    return {"producer_max_lag_s": lag, "producer_blocked_s": blocked}


# ----------------------------------------------------------------------------------------------
# Scenarios


KEYS = 50_000


def src(name, i, transport="tcp", rate=100_000, keys=None, start_ms=0, end_ms=60_000, schedule=None, **kw):
    d = {
        "name": name,
        "transport": transport,
        "rate": rate,
        "keys": keys or KEYS,
        "keymul": [2654435761, 2246822519, 3266489917, 668265263, 374761393, 1103515245, 22695477, 1664525, 69069, 134775813][i],
        "start_ms": start_ms,
        "end_ms": end_ms,
        "schedule": schedule or {"kind": "live"},
        "port": TCP_BASE_PORT + i,
        "topic": f"bp/{name}",
    }
    d.update(kw)
    return d


def scenarios(dur_s=60, rate=100_000, transport="tcp"):
    E = dur_s * 1000
    S = {}

    def two(name, sa=None, sb=None, query="join2", **kwb):
        S[name] = {
            "name": name,
            "query": query,
            "end_ms": E,
            "sources": [src("a", 0, transport, rate, end_ms=E, schedule=sa), src("b", 1, transport, rate, end_ms=E, schedule=sb, **kwb)],
        }

    two("steady")
    two("delay500ms", sb={"kind": "live", "delay_ms": 500})
    two("delay20s", sb={"kind": "live", "delay_ms": 20_000})
    two("jitter3s", sa={"kind": "jitter", "max_ms": 3000, "slot_s": 0.5, "seed": 1}, sb={"kind": "jitter", "max_ms": 3000, "slot_s": 0.5, "seed": 2})
    two("jitterfast", sa={"kind": "jitter", "max_ms": 2000, "slot_s": 0.1, "seed": 3}, sb={"kind": "jitter", "max_ms": 2000, "slot_s": 0.1, "seed": 4})
    two("sine10s", sa={"kind": "sine", "amp_ms": 10_000, "period_s": 20}, sb={"kind": "sine", "amp_ms": 10_000, "period_s": 20, "phase": math.pi})
    two("batch5s", sb={"kind": "batch", "every_s": 5})
    two("outage20s", sb={"kind": "outage", "from_s": 15, "to_s": 35})
    two("late20s", sb={"kind": "outage", "from_s": 0, "to_s": 20})
    two("idle20s", gaps_ms=[[20_000, 40_000]])
    two("replay30s", sb={"kind": "live"}, start_ms=-30_000)
    two("sparse")
    S["sparse"]["sources"][0].update(keys=1000)
    S["sparse"]["sources"][1].update(rate=20, flush_ms=1000, keys=1000)
    S["file_live"] = {
        "name": "file_live",
        "query": "join2",
        "end_ms": E,
        "sources": [src("a", 0, "file", rate, end_ms=E, file=f"bp/file_a_{rate}_{dur_s}.csv"), src("b", 1, transport, rate, end_ms=E)],
    }
    # Stacked join: c is the outer input.
    for name, sc in [("j3_steady", None), ("j3_cdelay20s", {"kind": "live", "delay_ms": 20_000}), ("j3_adelay20s", "a")]:
        srcs = [src("a", 0, transport, rate, end_ms=E), src("b", 1, transport, rate, end_ms=E), src("c", 2, transport, rate, end_ms=E)]
        if sc == "a":
            srcs[0]["schedule"] = {"kind": "live", "delay_ms": 20_000}
        elif sc:
            srcs[2]["schedule"] = sc
        S[name] = {"name": name, "query": "join3", "end_ms": E, "sources": srcs}
    # Aggregation over a union of 8 sources, one of them late or jittering.
    for name, sched in [("u8_steady", None), ("u8_onedelay20s", {"kind": "live", "delay_ms": 20_000}), ("u8_jitter3s", "jitter")]:
        srcs = []
        for i in range(8):
            sc = None
            if sched == "jitter":
                sc = {"kind": "jitter", "max_ms": 3000, "slot_s": 0.5, "seed": 10 + i}
            elif sched and i == 0:
                sc = sched
            srcs.append(src(f"s{i}", i, transport, rate // 4, keys=20_000, end_ms=E, schedule=sc))
        S[name] = {"name": name, "query": "unionagg", "end_ms": E, "sources": srcs}
    if transport != "tcp":
        for v in S.values():
            v["name"] = f"{transport}_{v['name']}"
        S = {v["name"]: v for v in S.values()}
    return S


def start_broker():
    """Runs mosquitto in this container's network namespace, so 127.0.0.1:1883 reaches it."""
    me = open("/etc/hostname").read().strip()
    name = f"bp-mosquitto-{me}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "--network", f"container:{me}", "eclipse-mosquitto:2",
         "mosquitto", "-c", "/mosquitto-no-auth.conf"],
        check=True, capture_output=True,
    )
    import atexit

    atexit.register(lambda: subprocess.run(["docker", "rm", "-f", name], capture_output=True))
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", 1883), timeout=0.2).close()
            return name
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("mosquitto did not start")


def broker_rss_mib(name):
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name], capture_output=True, text=True).stdout
    m = re.match(r"([0-9.]+)([KMG]i?B)", out.strip())
    if not m:
        return None
    scale = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1 / 1000, "MB": 1, "GB": 1000}.get(m.group(2), 1)
    return round(float(m.group(1)) * scale, 1)


BROKER = None


def main():
    global TOPOLOGY, KEYS, BROKER
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--scenarios", nargs="+", required=True)
    ap.add_argument("--gaps", nargs="+", type=int, default=[0, 1000, 10000])
    ap.add_argument("--transport", default="tcp")
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--rate", type=int, default=5_000)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--keys", type=int, default=50_000)
    ap.add_argument("--topology", default=TOPOLOGY)
    ap.add_argument("--worker-arg", action="append", default=[])
    ap.add_argument("--data-dir", default=os.environ.get("BP_DATA_DIR", "/tmp/bp-bench-data"), help="where file sources are generated")
    args = ap.parse_args()
    TOPOLOGY = args.topology
    KEYS = args.keys
    EXTRA_WORKER_ARGS.extend(args.worker_arg)
    catalog = scenarios(args.duration, args.rate, args.transport)
    data_dir = os.path.abspath(args.data_dir)
    os.makedirs(args.out, exist_ok=True)
    names = list(catalog) if args.scenarios == ["all"] else args.scenarios
    if args.transport == "mqtt":
        BROKER = start_broker()
    for rep in range(args.reps):
        for name in names:
            for gap in args.gaps:
                sc = dict(catalog[name])
                if args.reps > 1:
                    sc["name"] = f"{name}__r{rep}"
                t = time.time()
                summary = run(sc, gap, args.out, args.threads, data_dir, args.timeout)
                print(json.dumps(summary), flush=True)
                with open(os.path.join(args.out, "results.jsonl"), "a") as f:
                    f.write(json.dumps(summary) + "\n")


if __name__ == "__main__":
    main()
