#!/usr/bin/env python3
"""
Fan-out relay for the raspi-coffee telemetry stream.

WHY THIS EXISTS
---------------
The Pi serves its telemetry as (per Ricardo, 2026-07-17):

    tail -f <counters>.csv | nc -l ...

A shell pipe has exactly ONE reader. Every byte `tail` writes is consumed by a
single `nc`, which owns a single connection. There is no fan-out anywhere in
that pipeline, and no netcat flag can add one -- the limit is upstream of nc.
Consequences we measured against the real machine on 2026-07-16:

  * Only the FIRST client ever receives data. Later connections still complete
    the TCP handshake (the kernel parks them in the listen backlog even though
    nc never calls accept() again), so they look connected but receive zero
    bytes forever. Only one NES query could run at a time.
  * Connections dropped after ~11-130s. NES treats a closed source as
    End-of-Stream and STOPS the query permanently -- it does not reconnect.

This relay holds the ONE upstream connection the Pi can support and re-serves
that stream to as many local clients as we like. The Pi never sees more than one
connection and never knows the relay exists, so nothing changes on the Pi -- it
stays exactly as fragile as it is today.

    [ Pi: tail -f | nc ] <--1 conn-- [ ssh tunnel ] <-- [ THIS ] <--N-- [ NES ]

WHAT IT HANDLES, AND WHY EACH ONE IS NEEDED
-------------------------------------------
1. Fan-out. N downstream clients, one upstream connection.

2. Reconnect with backoff. Upstream drops are expected. Downstream sockets stay
   OPEN across an upstream outage -- that is the whole point. If we closed them,
   NES would see EoS and stop the query, which is the failure we are fixing.

3. Replay-dedup. `tail -f` replays the last ~10 lines of the file on start. If
   the Pi's supervisor re-runs the pipeline per connection, every reconnect
   would re-send rows we already forwarded, and NES's cumulative counters would
   appear to jump backwards. We drop any line seen in the last DEDUP_WINDOW
   lines. Safe because field 0 is an epoch-seconds timestamp, so genuine rows
   are never byte-identical.

4. Last-row replay to new clients (default ON, --no-replay-last to disable).
   The CSV is appended ONLY when a counter actually changes -- an idle machine
   writes nothing. Without this, a query starting during an idle period gets
   zero rows and its dashboard tile stays blank until someone uses the machine.
   Replaying the last real row makes tiles populate on connect. This is the last
   genuine reading, not a fabricated one.

5. Heartbeat (default OFF, --heartbeat SECONDS). Re-sends the last real row on
   an interval. This INFLATES row counts with repeated data -- it is off by
   default and should stay off unless you specifically want time-series tiles to
   keep ticking while the machine is idle.

Bytes are forwarded verbatim, including the Pi's CRLF line endings: we split on
b"\n" and re-attach it, so the trailing b"\r" stays part of the line exactly as
the Pi sent it. (NES's tuple_delimiter is a `char` and cannot be "\r\n"; the
stray \r lands on the last CSV field. See PI-SETUP-GUIDE.md.)

USAGE
-----
The relay takes over port 2222 so no query file needs editing. Move the tunnel
to a different local port and point the relay at it:

    ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
      -L 2223:raspi-coffee.bifold.tu-berlin.de:2222 tim@needmi-jh.dima.tu-berlin.de

    python3 scripts/pi-relay.py --upstream-port 2223 --listen-port 2222

Against the mock server instead (mock on 2223, relay on 2222):

    python3 scripts/coffe-server-pi-schema.py --port 2223
    python3 scripts/pi-relay.py --upstream-port 2223 --listen-port 2222
"""

import argparse
import asyncio
import collections
import contextlib
import logging
import sys
import time

LOG = logging.getLogger("pi-relay")

# tail -f replays the last 10 lines by default; 64 gives generous margin without
# any real memory cost.
DEDUP_WINDOW = 64

# If a downstream client cannot keep up, drop its OLDEST pending lines rather
# than stalling the upstream reader or disconnecting it. NES keeps up easily;
# this only guards against a wedged consumer.
CLIENT_QUEUE_MAX = 1000


class Relay:
    def __init__(self, args):
        self.args = args
        # Each downstream client owns a bounded queue of pending lines.
        self.clients: set[asyncio.Queue] = set()
        self.recent: collections.deque[bytes] = collections.deque(maxlen=DEDUP_WINDOW)
        self.last_line: bytes | None = None
        self.upstream_connected = False
        self.rows_in = 0
        self.rows_dropped_dup = 0
        self.rows_out = 0

    # ---------------------------------------------------------------- fan-out
    def broadcast(self, line: bytes) -> None:
        """Hand `line` to every connected client. Never blocks."""
        for queue in self.clients:
            if queue.full():
                # Drop the oldest so a slow client degrades rather than stalling
                # everyone else.
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(line)
        self.rows_out += len(self.clients)

    # --------------------------------------------------------------- upstream
    async def pump_upstream(self) -> None:
        """Hold one connection to the Pi. Reconnect forever, with backoff."""
        backoff = self.args.backoff_min
        while True:
            try:
                LOG.info(
                    "upstream: connecting to %s:%d",
                    self.args.upstream_host,
                    self.args.upstream_port,
                )
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        self.args.upstream_host, self.args.upstream_port
                    ),
                    timeout=self.args.connect_timeout,
                )
            except (OSError, asyncio.TimeoutError) as exc:
                LOG.warning(
                    "upstream: connect failed (%s); retrying in %.1fs", exc, backoff
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.args.backoff_max)
                continue

            self.upstream_connected = True
            backoff = self.args.backoff_min
            LOG.info("upstream: CONNECTED (downstream clients: %d)", len(self.clients))

            try:
                await self.read_lines(reader)
                LOG.warning("upstream: closed by peer (EOF)")
            except OSError as exc:
                LOG.warning("upstream: read error: %s", exc)
            finally:
                self.upstream_connected = False
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()

            # Downstream sockets are deliberately left OPEN here. NES must not
            # see EoS, or it stops the query and never restarts it.
            LOG.info(
                "upstream: disconnected; reconnecting in %.1fs "
                "(downstream clients kept open: %d)",
                backoff,
                len(self.clients),
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.args.backoff_max)

    async def read_lines(self, reader: asyncio.StreamReader) -> None:
        """Read one upstream connection to EOF, deduplicating tail's replay."""
        while True:
            line = await reader.readline()
            if not line:  # EOF
                return
            if not line.endswith(b"\n"):
                # Partial trailing line at EOF -- the connection died mid-row.
                # Forwarding it would hand NES a truncated CSV row.
                LOG.warning("upstream: discarding partial line at EOF: %r", line[:80])
                return

            self.rows_in += 1
            payload = line.rstrip(b"\n")

            if payload in self.recent:
                # tail -f re-emitted a row we already forwarded.
                self.rows_dropped_dup += 1
                LOG.debug("dedup: dropped replayed row")
                continue

            self.recent.append(payload)
            self.last_line = line
            self.broadcast(line)

    # ------------------------------------------------------------- downstream
    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        queue: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_QUEUE_MAX)
        self.clients.add(queue)
        LOG.info("client %s: connected (total: %d)", peer, len(self.clients))

        # The CSV only grows when a counter changes, so an idle machine would
        # leave a fresh client with nothing to read. Send the last real row so
        # its tile populates immediately.
        if self.args.replay_last and self.last_line is not None:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(self.last_line)

        try:
            while True:
                line = await queue.get()
                writer.write(line)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            LOG.info("client %s: disconnected (%s)", peer, exc)
        except asyncio.CancelledError:
            raise
        finally:
            self.clients.discard(queue)
            LOG.info("client %s: gone (remaining: %d)", peer, len(self.clients))
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    # ------------------------------------------------------------ heartbeat
    async def pump_heartbeat(self) -> None:
        """Re-send the last real row on an interval. Opt-in; see module docs."""
        while True:
            await asyncio.sleep(self.args.heartbeat)
            if self.last_line is not None:
                LOG.debug("heartbeat: re-sending last row")
                self.broadcast(self.last_line)

    # ----------------------------------------------------------------- stats
    async def pump_stats(self) -> None:
        started = time.monotonic()
        while True:
            await asyncio.sleep(self.args.stats_interval)
            LOG.info(
                "stats: up=%.0fs upstream=%s clients=%d rows_in=%d dup_dropped=%d "
                "rows_forwarded=%d",
                time.monotonic() - started,
                "CONNECTED" if self.upstream_connected else "DISCONNECTED",
                len(self.clients),
                self.rows_in,
                self.rows_dropped_dup,
                self.rows_out,
            )

    # ------------------------------------------------------------------ main
    async def run(self) -> None:
        server = await asyncio.start_server(
            self.handle_client, self.args.listen_host, self.args.listen_port
        )
        LOG.info(
            "listening on %s:%d -> upstream %s:%d",
            self.args.listen_host,
            self.args.listen_port,
            self.args.upstream_host,
            self.args.upstream_port,
        )
        LOG.info(
            "replay-last=%s heartbeat=%s",
            "on" if self.args.replay_last else "off",
            f"{self.args.heartbeat}s" if self.args.heartbeat else "off",
        )

        tasks = [
            asyncio.create_task(self.pump_upstream()),
            asyncio.create_task(self.pump_stats()),
        ]
        if self.args.heartbeat:
            tasks.append(asyncio.create_task(self.pump_heartbeat()))

        async with server:
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                pass


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Fan-out relay for the single-client raspi-coffee TCP stream.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--upstream-host", default="127.0.0.1",
                   help="where the Pi stream arrives (ssh tunnel). default: %(default)s")
    p.add_argument("--upstream-port", type=int, default=2223,
                   help="tunnel's local port. default: %(default)s")
    p.add_argument("--listen-host", default="0.0.0.0",
                   help="0.0.0.0 so Docker's host.docker.internal can reach it. "
                        "default: %(default)s")
    p.add_argument("--listen-port", type=int, default=2222,
                   help="port NES dials; keep 2222 so queries need no edit. "
                        "default: %(default)s")
    p.add_argument("--connect-timeout", type=float, default=10.0,
                   help="upstream connect timeout, seconds. default: %(default)s")
    p.add_argument("--backoff-min", type=float, default=1.0,
                   help="initial reconnect delay, seconds. default: %(default)s")
    p.add_argument("--backoff-max", type=float, default=30.0,
                   help="max reconnect delay, seconds. default: %(default)s")
    p.add_argument("--no-replay-last", dest="replay_last", action="store_false",
                   help="do NOT send the last known row to a newly connected "
                        "client (default: it is sent, so tiles populate while idle)")
    p.add_argument("--heartbeat", type=float, default=0.0, metavar="SECONDS",
                   help="re-send the last real row every N seconds. OFF by "
                        "default; inflates row counts with repeated data.")
    p.add_argument("--stats-interval", type=float, default=60.0,
                   help="seconds between stats lines. default: %(default)s")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    return p.parse_args(argv)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        asyncio.run(Relay(args).run())
    except KeyboardInterrupt:
        LOG.info("shutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
