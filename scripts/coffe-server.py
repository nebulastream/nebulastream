"""
Eversys Telemetry TCP Server — Nebula Stream Edition
=====================================================
Emits MachineCounters snapshots as newline-delimited CSV rows.
Schema matches the real serial protocol exactly — all fields are integers.

One TCP port per machine. Nes Source connects as the TCP client and
reads a continuous stream of counter snapshots at a fixed interval.

Admin REST API (port 8000) for manual testing:
  POST /simulate/brew/{machineId}       — trigger one brew cycle
  POST /simulate/clean/{machineId}      — trigger one cleaning cycle
  POST /simulate/brew/{machineId}?n=5   — trigger N brew cycles
  GET  /status/{machineId}             — current counter snapshot
  GET  /status                         — all machines

Usage:
  python tcp_server.py [--interval 5.0] [--base-port 9101] [--admin-port 8000]

Ports (defaults):
  Machine 101 -> 9101
  Machine 102 -> 9102
  Machine 103 -> 9103
  Admin HTTP  -> 8000
"""

import argparse
import asyncio
import csv
import io
import json
import logging
import random
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("tcp_server")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def json_response(writer: asyncio.StreamWriter, status: int, body: dict) -> None:
    payload = json.dumps(body).encode()
    reason = "OK" if status == 200 else "Not Found" if status == 404 else "Bad Request"
    response = (
                   f"HTTP/1.1 {status} {reason}\r\n"
                   f"Content-Type: application/json\r\n"
                   f"Content-Length: {len(payload)}\r\n"
                   f"Connection: close\r\n"
                   f"\r\n"
               ).encode() + payload
    writer.write(response)


# ---------------------------------------------------------------------------
# Machine state
# Units match the real serial protocol:
#   Water  -> ticks  (1 tick ~ 0.5 ml)
#   Beans  -> grams  (int)
#   Times  -> centiseconds (cs),  1 cs = 10 ms
# ---------------------------------------------------------------------------

@dataclass
class MachineState:
    machine_id: int

    # Water (ticks)
    total_water_quantity_ticks: int = 0
    total_water_products_ticks: int = 0
    total_water_rinse_ticks: int = 0
    total_water_cleaning_ticks: int = 0
    rfu: int = 0

    # Beans (grams)
    total_coffee_bean_quantity_g: int = 0
    total_coffee_beans_left_rear_g: int = 0
    total_coffee_beans_right_front_g: int = 0
    total_coffee_powder_chute_g: int = 0

    # Cycle counts
    cleaning_counter: int = 0
    brewcycles_left: int = 0
    brewcycles_right: int = 0
    cycles_rinse_clean_left: int = 0
    cycles_rinse_clean_right: int = 0

    # Times (centiseconds)
    time_milk_cs: int = 0
    heating_time_boiler_left_cs: int = 0
    heating_time_boiler_right_cs: int = 0
    time_hot_water_cs: int = 0
    time_steam_left_cs: int = 0
    time_steam_right_cs: int = 0
    heat_time_steam_boiler_1_cs: int = 0
    heat_time_steam_boiler_2_cs: int = 0
    time_air_pump_cs: int = 0

    # Milk cycles
    milk_cycles_left: int = 0
    milk_cycles_right: int = 0

    # Reset timestamp
    reset_date_day: int = field(default_factory=lambda: datetime.now().day)
    reset_date_month: int = field(default_factory=lambda: datetime.now().month)
    reset_date_year: int = field(default_factory=lambda: datetime.now().year % 100)
    rfu2: int = 0
    reset_time_second: int = field(default_factory=lambda: datetime.now().second)
    reset_time_minute: int = field(default_factory=lambda: datetime.now().minute)
    reset_time_hour: int = field(default_factory=lambda: datetime.now().hour)
    rfu3: int = 0

    # Milk steam times (cs)
    time_milk_steam_left_cs: int = 0
    time_milk_steam_right_cs: int = 0

    # Grinder + absolute counter
    total_coffee_beans_grinder_adjustment_g: int = 0
    absolute_counter: int = 0

    # Powder
    time_powder_cs: int = 0
    total_powder_quantity_left_g: int = 0
    total_powder_quantity_right_g: int = 0

    # Hopper 3 & 4
    total_coffee_beans_3_g: int = 0
    total_coffee_beans_4_g: int = 0


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def simulate_brew(m: MachineState) -> None:
    side = random.choice(["left", "right"])
    milk = random.random() < 0.25
    hopper = random.choices(
        ["left_rear", "right_front", "hopper3", "hopper4"],
        weights=[45, 45, 9, 1],
    )[0]

    beans_g = random.randint(7, 11)
    m.total_coffee_bean_quantity_g += beans_g
    if hopper == "left_rear":
        m.total_coffee_beans_left_rear_g += beans_g
    elif hopper == "right_front":
        m.total_coffee_beans_right_front_g += beans_g
    elif hopper == "hopper3":
        m.total_coffee_beans_3_g += beans_g
    else:
        m.total_coffee_beans_4_g += beans_g

    m.total_coffee_beans_grinder_adjustment_g += random.randint(0, 1)

    water_ticks = random.randint(120, 240)
    m.total_water_products_ticks += water_ticks
    m.total_water_quantity_ticks += water_ticks

    m.time_air_pump_cs += random.randint(30, 150)

    heat_cs = random.randint(200, 800)
    if side == "left":
        m.brewcycles_left += 1
        m.heating_time_boiler_left_cs += heat_cs
    else:
        m.brewcycles_right += 1
        m.heating_time_boiler_right_cs += heat_cs

    if milk:
        m.time_milk_cs += random.randint(300, 1000)
        m.heat_time_steam_boiler_1_cs += random.randint(50, 200)
        steam_cs = random.randint(100, 600)
        milk_steam_cs = random.randint(500, 2000)
        if side == "left":
            m.milk_cycles_left += 1
            m.time_steam_left_cs += steam_cs
            m.time_milk_steam_left_cs += milk_steam_cs
        else:
            m.milk_cycles_right += 1
            m.time_steam_right_cs += steam_cs
            m.time_milk_steam_right_cs += milk_steam_cs

    m.absolute_counter += 1


def simulate_cleaning(m: MachineState) -> None:
    side = random.choice(["left", "right"])
    cleaning_ticks = random.randint(500, 2000)
    rinse_ticks = random.randint(100, 600)
    m.total_water_cleaning_ticks += cleaning_ticks
    m.total_water_rinse_ticks += rinse_ticks
    m.total_water_quantity_ticks += cleaning_ticks + rinse_ticks
    m.cleaning_counter += 1
    if side == "left":
        m.cycles_rinse_clean_left += 1
    else:
        m.cycles_rinse_clean_right += 1


# ---------------------------------------------------------------------------
# CSV serialisation — field order is the NES schema contract, do not reorder
# ---------------------------------------------------------------------------

FIELDS = [
    "machine_id", "ts_ms",
    "total_water_quantity_ticks", "total_water_products_ticks",
    "total_water_rinse_ticks", "total_water_cleaning_ticks",
    "rfu", "total_coffee_bean_quantity_g",
    "total_coffee_beans_left_rear_g", "total_coffee_beans_right_front_g",
    "total_coffee_powder_chute_g", "cleaning_counter",
    "brewcycles_left", "brewcycles_right",
    "cycles_rinse_clean_left", "cycles_rinse_clean_right",
    "time_milk_cs", "heating_time_boiler_left_cs", "heating_time_boiler_right_cs",
    "time_hot_water_cs", "time_steam_left_cs", "time_steam_right_cs",
    "heat_time_steam_boiler_1_cs", "heat_time_steam_boiler_2_cs",
    "time_air_pump_cs", "milk_cycles_left", "milk_cycles_right",
    "reset_date_day", "reset_date_month", "reset_date_year",
    "rfu2", "reset_time_second", "reset_time_minute", "reset_time_hour", "rfu3",
    "time_milk_steam_left_cs", "time_milk_steam_right_cs",
    "total_coffee_beans_grinder_adjustment_g", "absolute_counter",
    "time_powder_cs", "total_powder_quantity_left_g", "total_powder_quantity_right_g",
    "total_coffee_beans_3_g", "total_coffee_beans_4_g",
]


def snapshot_to_csv_bytes(m: MachineState) -> bytes:
    values = []
    for f in FIELDS:
        if f == "ts_ms":
            values.append(now_ms())
        else:
            values.append(getattr(m, f))
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerow(values)
    return buf.getvalue().encode()


def machine_snapshot_dict(m: MachineState) -> dict:
    d = {f: getattr(m, f) for f in FIELDS if f != "ts_ms"}
    d["ts_ms"] = now_ms()
    return d


# ---------------------------------------------------------------------------
# Per-machine TCP handler — unchanged, NES connects here
# ---------------------------------------------------------------------------

async def machine_handler(
        machine: MachineState,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        interval_sec: float,
) -> None:
    peer = writer.get_extra_info("peername")
    log.info("Machine %d: client connected from %s", machine.machine_id, peer)
    try:
        while True:
            for _ in range(random.randint(0, 3)):
                simulate_brew(machine)
            if random.random() < 0.10:
                simulate_cleaning(machine)
            writer.write(snapshot_to_csv_bytes(machine))
            log.debug(
                "Machine %d: snapshot (absolute_counter=%d)",
                machine.machine_id, machine.absolute_counter,
            )
            await writer.drain()
            await asyncio.sleep(interval_sec)
    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        log.info("Machine %d: client disconnected", machine.machine_id)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_machine_server(
        machine: MachineState, port: int, interval_sec: float
) -> None:
    async def _handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await machine_handler(machine, r, w, interval_sec)

    server = await asyncio.start_server(_handle, host="0.0.0.0", port=port)
    log.info("Machine %d listening on port %d", machine.machine_id, port)
    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# Admin HTTP server — minimal raw HTTP, no extra dependencies
# ---------------------------------------------------------------------------

async def admin_handler(
        machines: Dict[int, MachineState],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        request_line = request_line.decode().strip()
        if not request_line:
            return

        # Drain headers
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line in (b"\r\n", b"\n", b""):
                break

        parts = request_line.split()
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]

        # Strip query string
        query = ""
        if "?" in path:
            path, query = path.split("?", 1)
        params = dict(p.split("=", 1) for p in query.split("&") if "=" in p)

        # ---- Route: GET /status ----
        if method == "GET" and path == "/status":
            body = {mid: machine_snapshot_dict(m) for mid, m in machines.items()}
            json_response(writer, 200, body)

        # ---- Route: GET /status/{machineId} ----
        elif method == "GET" and path.startswith("/status/"):
            try:
                mid = int(path.split("/")[2])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"})
                return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"})
                return
            json_response(writer, 200, machine_snapshot_dict(m))

        # ---- Route: POST /simulate/brew/{machineId} ----
        elif method == "POST" and path.startswith("/simulate/brew/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"})
                return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"})
                return
            n = max(1, int(params.get("n", 1)))
            for _ in range(n):
                simulate_brew(m)
            log.info("Admin: triggered %d brew(s) on machine %d (absolute_counter=%d)",
                     n, mid, m.absolute_counter)
            json_response(writer, 200, {
                "ok": True, "machineId": mid, "brews": n,
                "absolute_counter": m.absolute_counter,
            })

        # ---- Route: POST /simulate/clean/{machineId} ----
        elif method == "POST" and path.startswith("/simulate/clean/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"})
                return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"})
                return
            simulate_cleaning(m)
            log.info("Admin: triggered cleaning on machine %d (cleaning_counter=%d)",
                     mid, m.cleaning_counter)
            json_response(writer, 200, {
                "ok": True, "machineId": mid,
                "cleaning_counter": m.cleaning_counter,
            })

        else:
            json_response(writer, 404, {"error": "unknown route"})

    except asyncio.TimeoutError:
        pass
    except Exception as e:
        log.warning("Admin handler error: %s", e)
    finally:
        try:
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_admin_server(machines: Dict[int, MachineState], port: int) -> None:
    async def _handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await admin_handler(machines, r, w)

    server = await asyncio.start_server(_handle, host="0.0.0.0", port=port)
    log.info("Admin HTTP listening on port %d", port)
    log.info("  GET  http://localhost:%d/status", port)
    log.info("  GET  http://localhost:%d/status/{machineId}", port)
    log.info("  POST http://localhost:%d/simulate/brew/{machineId}", port)
    log.info("  POST http://localhost:%d/simulate/brew/{machineId}?n=10", port)
    log.info("  POST http://localhost:%d/simulate/clean/{machineId}", port)
    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

MACHINE_IDS = [101, 102, 103]


async def main(base_port: int, interval_sec: float, admin_port: int) -> None:
    machines: Dict[int, MachineState] = {
        mid: MachineState(machine_id=mid) for mid in MACHINE_IDS
    }
    port_map = {mid: base_port + i for i, mid in enumerate(MACHINE_IDS)}

    log.info("Starting Eversys TCP telemetry server")
    log.info("Emit interval : %.1f s", interval_sec)
    for mid, port in port_map.items():
        log.info("  Machine %d -> 0.0.0.0:%d", mid, port)

    await asyncio.gather(*[
                              asyncio.create_task(run_machine_server(machines[mid], port_map[mid], interval_sec))
                              for mid in MACHINE_IDS
                          ] + [
                              asyncio.create_task(run_admin_server(machines, admin_port))
                          ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eversys TCP telemetry server for Nebula Stream")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="Seconds between snapshots per machine (default: 5.0)")
    parser.add_argument("--base-port", type=int, default=9101,
                        help="Base port; 101->base, 102->base+1, ... (default: 9101)")
    parser.add_argument("--admin-port", type=int, default=8000,
                        help="Admin HTTP API port (default: 8000)")
    args = parser.parse_args()
    try:
        asyncio.run(main(
            base_port=args.base_port,
            interval_sec=args.interval,
            admin_port=args.admin_port,
        ))
    except KeyboardInterrupt:
        log.info("Server stopped.")