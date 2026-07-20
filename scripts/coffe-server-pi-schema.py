"""
Eversys Telemetry TCP Server — REAL-PI SCHEMA edition
=====================================================

A drop-in stand-in for the real coffee-machine Pi
(raspi-coffee.bifold.tu-berlin.de:2222). It emits the EXACT wire format we
captured off the real hardware on 2026-07-16, so queries developed against this
server work unchanged when you swap the real Pi back in.

WHY THIS FILE EXISTS — schema changes vs. the old scripts/coffe-server.py
------------------------------------------------------------------------
The original coffe-server.py was written before anyone had seen the real Pi.
Once we read real rows off it, three differences showed up. Each one silently
corrupts NES parsing if you get it wrong:

 1. 43 FIELDS, NOT 44 — there is NO machine_id.
    The old mock sent machine_id as field 1. The real Pi does not send it at
    all; its first field is the timestamp. An extra leading field shifts every
    subsequent column by one, so every value lands in the wrong schema slot —
    and nothing errors, you just get plausible-looking wrong numbers.

 2. THE TIMESTAMP IS EPOCH *SECONDS*, NOT MILLISECONDS.
    Real sample: 1784211858 (= 2026-07-16). The field is still *named* ts_ms,
    which is misleading, but it carries seconds.
    This matters enormously for NES: a windowed query always interprets its
    timestamp field as MILLISECONDS (TimeCharacteristic.cpp:41 defaults to
    TimeUnit(1), and the SQL grammar's `timestampParameter` accepts no unit).
    So `WINDOW TUMBLING(ts_ms, SIZE 10 SEC)` means 10000 ticks = 10000 seconds
    ~= 2.8 HOURS of data before ONE window closes. That is why every query
    shipped against this schema is a passthrough with no WINDOW clause.
    (If you ever do need a window: multiply first, e.g.
     `ts_ms * UINT64(1000) AS ts_real_ms` in a subquery — that pattern is
     confirmed active in
     nes-systests/operator/projection/ProjectionWithFilterAndMap.test:37.)

 3. ROWS END WITH CRLF (\\r\\n), NOT \\n.
    NES's tuple_delimiter is typed `char` (TCPSource.hpp:121-123), so "\\r\\n"
    cannot be expressed. With "\\n" the stray \\r sticks to the LAST field
    (total_coffee_beans_4_g). Don't select that field unless you handle it.

Verified real row (2026-07-16):
  1784211858,584918,209065,137436,236916,0,26871,11629,12819,0,89,1957,0,494,0,
  102866,0,0,104,0,0,0,0,79760,685,0,12,6,25,0,23,36,15,0,187506,0,435,2622,0,
  0,0,1988,0

The 43-field mapping was confirmed arithmetically against that row, not guessed:
  total_coffee_bean_quantity_g (26871)
    = left_rear (11629) + right_front (12819) + beans_3 (1988)
      + grinder_adjustment (435)   ->  26871 exactly
  reset_date = 12/6/25, reset_time = 15:36:23  (both plausible)
  water quantity (584918) ~= products (209065) + rinse (137436)
                             + cleaning (236916)

BEHAVIOUR MODEL — calibrated against 24 DAYS OF REAL DATA
---------------------------------------------------------
Every constant below was measured from
scripts/coffe_queries/tcp/testv2/dashboard/machine_counters_whole.csv
(4839 rows, 2026-06-26 -> 2026-07-20, 723 real products). Earlier versions of
this file invented the numbers, and the invented ones were wrong by 3-6x —
notably water-per-product (real median 40 ticks, the old mock used 120-240).

The single most important correction: THE REAL MACHINE IS SLOW AND MOSTLY IDLE.
5-80 drinks per DAY, sampled every ~8s. The old mock fired 0-3 brews per second,
which is ~100x reality and made every dashboard tile look plausible while
hiding the real problem: on real data, short time windows are almost always
empty. Anything that looks good here must also look good at 40 drinks/day.

Measured constants. Note these are 24-day TOTALS divided by event count, NOT
per-step medians. That distinction is not pedantic — it moved two constants by
2x and 20x. Rinse and cleaning water accrue on samples where the cycle counter
does not move, so measuring only the increment steps undercounts badly: the
per-step median says 136 ticks per deep clean, the totals say 2770. Trusting
the median would have shrunk the doughnut's largest slice to nothing.

  sample interval      8 s
  products             ~30/weekday; only 73% of them run a brew cycle
  brewing product      +1 absolute_counter, +1 brewcycles_left, ~12 g beans,
                       ~42 water product ticks
  non-brew product     +1 absolute_counter only, ~135 water ticks (hot water) —
                       27% of products, and the machine's thirstiest
  milk drink           ~18% of products; +1 milk_cycles_left, +115 cs time_milk
  rinse cycle          ~1 per 3.7 brews; ~215 rinse ticks
  deep clean           ~1 per day; ~2770 cleaning ticks (~1 L)
  bean hopper split    left_rear 55% / right_front 36% / hopper3 7% /
                       grinder_adjustment 2%
  hourly profile       06:00-17:00 UTC only, peaking 11:00 UTC (20.8%)
  weekday profile      Mon-Thu busy, Fri ~half, Sat/Sun ~idle

Simulated output matches every one of those aggregates to within 15% over a
56-day run, with both structural identities at residual 0.

Two structural identities hold EXACTLY in the real data and are enforced here;
the dashboard's doughnut and bar totals depend on them:
  total_coffee_bean_quantity_g = left_rear + right_front + beans_3
                                 + grinder_adjustment           (residual 0)
  total_water_quantity_ticks   = products + rinse + cleaning + a CONSTANT offset
The water offset is 1220-1501 ticks and does NOT accumulate (it stepped once,
on 2026-07-08, and was flat either side). So it is a calibration artifact, NOT
a hidden "purge" counter — there is no purge counter on this machine.

ALL 43 FIELDS ARE ALWAYS EMITTED, including the 18 that are permanently zero on
the real machine (rfu/rfu2/rfu3, everything *_right, both boilers, steam,
powder, chute, beans_4). The Pi's server cannot be edited, so the wire format is
fixed; "dead" means "don't chart it", never "don't send it". Those fields are
pinned at 0 here so the mock cannot make a dead field look alive.

FAULT INJECTION — on by default, because it found real bugs
-----------------------------------------------------------
The real capture contains 35 all-zero rows and 14 counter resets-to-zero
(reconnects/reboots). A naive windowed MAX-MIN spanning one reads a garbage
spike — this is exactly what once made an analysis report "2671 products in one
day" when the true figure was 9. The simulator reproduces both, rarely, so
queries and dashboard callbacks get tested against them. Disable with
--no-faults when you need a clean stream.

DELIBERATE DIFFERENCES FROM THE REAL PI — this server is better on purpose
--------------------------------------------------------------------------
 * Serves MANY clients at once. The real Pi appears single-client: the first
   connection gets data, every later one is accepted then closed with zero
   bytes. That limit meant only ONE NES query could run at a time.
 * Never hangs up. The real Pi closes after ~11-130s, which NES sees as
   End-of-Stream and uses to kill the query. This server streams forever.
 * --speedup compresses the clock so a demo does not need a real workday to
   show a trend. It changes ONLY how fast simulated time advances; per-event
   increments stay at their measured real values. Default 1.0 = real time.

SWAPPING BACK TO THE REAL PI
----------------------------
This listens on 2222 — the same port the SSH tunnel uses — so the query YAMLs
need NO changes. To switch: stop this server, then start the tunnel:

  ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \\
    -L 2222:raspi-coffee.bifold.tu-berlin.de:2222 tim@needmi-jh.dima.tu-berlin.de

Full runbook: scripts/coffe_queries/tcp/testv2/PI-SETUP-GUIDE.md

Usage:
  python3 scripts/coffe-server-pi-schema.py [--interval 8.0] [--speedup 1.0]
                                            [--port 2222] [--admin-port 8000]
                                            [--no-faults]

Admin REST API (port 8000) — drive a live demo by hand:
  POST /simulate/brew/101              — one single-shot coffee
  POST /simulate/brew/101?n=5          — N of them
  POST /simulate/brew/101?kind=double  — double shot (14-16 g)
  POST /simulate/brew/101?milk=1       — with milk (any kind=)
  POST /simulate/rinse/101             — one rinse cycle
  POST /simulate/clean/101             — one deep-clean cycle
  POST /simulate/reset/101             — zero every counter (reset fault)
  POST /simulate/dropout/101           — emit N all-zero rows (?n=3)
  GET  /status/101                     — current counter snapshot
  GET  /status                         — all machines
"""

import argparse
import asyncio
import csv
import io
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("pi_schema_server")


def now_epoch_seconds() -> int:
    """The real Pi sends epoch SECONDS in the (misleadingly named) ts_ms field."""
    return int(time.time())


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
# Machine state.
# Defaults are the LAST REAL ROW of machine_counters_whole.csv (2026-07-20
# 08:11:10 UTC), so the stream continues exactly where the real capture stopped
# rather than starting from zero or from an older snapshot.
#
# Units match the real serial protocol:
#   Water -> ticks (~0.36 ml/tick, measured);  Beans -> grams;
#   Times -> centiseconds
#
# The 18 fields marked DEAD are permanently zero on the real machine. They stay
# in the dataclass and on the wire (the Pi's format is fixed) but NOTHING here
# may ever increment them — a mock that animates a dead field teaches the
# dashboard to chart a line the real machine will never draw.
# ---------------------------------------------------------------------------

@dataclass
class MachineState:
    machine_id: int  # kept for the admin API only — NOT emitted on the wire

    total_water_quantity_ticks: int = 597929
    total_water_products_ticks: int = 211767
    total_water_rinse_ticks: int = 140777
    total_water_cleaning_ticks: int = 243884
    rfu: int = 0  # DEAD

    total_coffee_bean_quantity_g: int = 27265
    total_coffee_beans_left_rear_g: int = 11885
    total_coffee_beans_right_front_g: int = 12918
    total_coffee_powder_chute_g: int = 0  # DEAD

    cleaning_counter: int = 92
    brewcycles_left: int = 1996
    brewcycles_right: int = 0  # DEAD — single-brewer machine
    cycles_rinse_clean_left: int = 518
    cycles_rinse_clean_right: int = 0  # DEAD

    time_milk_cs: int = 103361
    heating_time_boiler_left_cs: int = 0  # DEAD — not reported by this machine
    heating_time_boiler_right_cs: int = 0  # DEAD
    time_hot_water_cs: int = 104  # effectively static (2 distinct values in 24d)
    time_steam_left_cs: int = 0  # DEAD — steam wand never used
    time_steam_right_cs: int = 0  # DEAD
    heat_time_steam_boiler_1_cs: int = 0  # DEAD
    heat_time_steam_boiler_2_cs: int = 0  # DEAD
    time_air_pump_cs: int = 80308

    milk_cycles_left: int = 690
    milk_cycles_right: int = 0  # DEAD — single milk system

    reset_date_day: int = 12
    reset_date_month: int = 6
    reset_date_year: int = 25
    rfu2: int = 0  # DEAD
    reset_time_second: int = 23
    reset_time_minute: int = 36
    reset_time_hour: int = 15
    rfu3: int = 0  # DEAD

    time_milk_steam_left_cs: int = 190524
    time_milk_steam_right_cs: int = 0  # DEAD

    total_coffee_beans_grinder_adjustment_g: int = 435
    absolute_counter: int = 2671

    time_powder_cs: int = 0  # DEAD — no powder chute
    total_powder_quantity_left_g: int = 0  # DEAD
    total_powder_quantity_right_g: int = 0  # DEAD

    total_coffee_beans_3_g: int = 2027
    total_coffee_beans_4_g: int = 0  # DEAD — hopper 4 not fitted

    # Not on the wire — fault-injection bookkeeping.
    #
    # A deadline, not a row count: a real dropout is a property of the machine
    # at a moment in time, so every connected client must see it. A counter gets
    # decremented by whichever client happens to emit next, so with six queries
    # attached a "3 row" dropout would be split one row each instead of three
    # rows each — the opposite of what we want to test.
    dropout_until: float = 0.0


# --- Measured workload profile (see module docstring for provenance) ---------

# Share of daily products by hour, UTC. Zero outside 06:00-17:00; peak at 11:00
# UTC (= 13:00 Berlin, the lunch rush). Sums to 1.0.
HOURLY_PRODUCT_SHARE = [
    0.0, 0.0, 0.0, 0.0, 0.0, 0.0014, 0.0373, 0.1065,
    0.0871, 0.0816, 0.1176, 0.2075, 0.1231, 0.0775,
    0.0858, 0.0235, 0.0332, 0.0180, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
]

# Products per day by weekday (Mon=0), from 723 real products over 24 days.
PRODUCTS_PER_DAY = [42, 51, 47, 38, 21, 4, 4]

# Bean grams drawn per hopper, matching the measured 55/36/7/2 split. The
# grinder-adjustment share is real: it is beans discarded while the grinder
# recalibrates, which is why it belongs in the total but not in a "drinks" chart.
HOPPER_WEIGHTS = [
    ("total_coffee_beans_left_rear_g", 55),
    ("total_coffee_beans_right_front_g", 36),
    ("total_coffee_beans_3_g", 7),
    ("total_coffee_beans_grinder_adjustment_g", 2),
]

# --- Drink mix ---------------------------------------------------------------
#
# UNRESOLVED: the real data supports two incompatible readings, and they differ
# by ~30% in beans and brews. The aggregates win here, because they are what the
# dashboard actually displays. Worth settling with whoever knows the machine.
#
#   Aggregate reading (24-day totals):
#     723 products, 530 brew cycles, 6355 g beans
#     -> 27% of products never brew, and a brewing product averages 12.0 g
#
#   Per-event reading (the 271 steps where absolute_counter advanced by
#   exactly 1):
#     bean histogram peaks hard at 9 g; only 24/271 are >=13 g; only 10/271
#     grind nothing -> 89% single shots, 4% no-bean
#
# They cannot both be true. The most likely explanation is that a double shot
# increments absolute_counter TWICE but brewcycles_left ONCE, which would make
# "products" mean cups and "brewcycles" mean grinds — that alone accounts for
# the missing 193. If so, TopDrinks must classify on brewcycles, not products.
# Until that is confirmed, this models the aggregate.
#
# Note the per-event restriction is still essential for anything measured
# per-event: across all steps 723 products arrive in 477 increments, so a 15 g
# delta is usually TWO singles. Reading every >=13 g delta as one double gives
# 42% doubles and 11.5 g/product against a real 8.7.
NO_BREW_SHARE = 0.267      # (723-530)/723 — product with no brew cycle, no beans
DOUBLE_SHARE = 0.52        # among BREWING products, to hit the real 12.0 g/brew
MILK_SHARE = 0.18          # 131 milk cycles / 723 products, aggregate
# NB: the same 271 clean steps show a 33% milk rate. The aggregate is used
# because it is what any dashboard total will reproduce; the gap means milk
# drinks are under-represented in busy multi-product bursts.
RINSE_PER_BREW = 1 / 3.7   # 144 rinse cycles / 530 brews
CLEANS_PER_DAY = 0.83      # 20 deep cleans / 24 days


def _add_beans(m: MachineState, grams: int) -> None:
    """Distribute grams to one hopper, preserving the exact bean identity.

    total = left_rear + right_front + beans_3 + grinder_adjustment holds with
    residual 0 in the real data, and the dashboard's bar totals rely on it, so
    the parent counter and exactly one child are always bumped together.
    """
    names = [n for n, _ in HOPPER_WEIGHTS]
    weights = [w for _, w in HOPPER_WEIGHTS]
    target = random.choices(names, weights=weights)[0]
    setattr(m, target, getattr(m, target) + grams)
    m.total_coffee_bean_quantity_g += grams


def _add_milk(m: MachineState) -> None:
    """One milk cycle. Single milk system: only the _left counters exist."""
    m.milk_cycles_left += 1
    m.time_milk_cs += random.randint(90, 140)
    m.time_milk_steam_left_cs += random.randint(280, 400)
    m.time_air_pump_cs += random.randint(45, 80)


def simulate_brew(m: MachineState, kind: str = "single", milk: bool | None = None) -> None:
    """One product. Increments match the real machine's measured per-event values.

    `kind`: "single" (7-10 g, 84% of real products), "double" (14-16 g, 9%), or
    "nobeans" (hot water / milk-only, 4%). `milk` defaults to the measured rate.
    """
    if milk is None:
        milk = random.random() < MILK_SHARE

    if kind == "nobrew":
        # A real product that grinds nothing and runs no brew cycle: hot water,
        # or milk on its own. 27% of real products. absolute_counter still moves.
        #
        # These are the machine's THIRSTIEST products. The 24-day totals only
        # reconcile if they are: 48526 product ticks over 723 products is 67
        # each, but a brew is only ~42, so the remaining ~26000 ticks have to
        # come from the 193 non-brew products — ~135 each, about a mug of hot
        # water. Omitting this made water-per-product read 34% low.
        hot_water_ticks = random.randint(115, 155)
        m.total_water_products_ticks += hot_water_ticks
        m.total_water_quantity_ticks += hot_water_ticks
        m.absolute_counter += 1
        if milk:
            _add_milk(m)
        return

    if kind == "double":
        _add_beans(m, random.randint(14, 16))
    else:
        # Peaked at 9 g, matching the real histogram (7:51, 8:55, 9:91, 10:27).
        _add_beans(m, random.choices([7, 8, 9, 10], weights=[51, 55, 91, 27])[0])

    # ~42 ticks for an actual brew (real per-product median is 40).
    water_ticks = random.randint(30, 55)
    m.total_water_products_ticks += water_ticks
    m.total_water_quantity_ticks += water_ticks

    # Single-brewer machine: brewcycles_right is DEAD and stays 0.
    m.brewcycles_left += 1
    m.absolute_counter += 1

    if milk:
        _add_milk(m)

    # Rinse is driven BY brewing, not scheduled: ~1 rinse per 3.7 brews.
    if random.random() < RINSE_PER_BREW:
        simulate_rinse(m)


def simulate_rinse(m: MachineState) -> None:
    """Short automatic rinse — ~215 ticks, ~1 per 3.7 brews.

    215 = 30996 total rinse ticks / 144 rinse cycles. Do NOT use the per-step
    median (101): rinse water also accrues on steps where the cycle counter does
    not move, so measuring only the increment steps undercounts by half.
    """
    ticks = random.randint(180, 250)
    m.total_water_rinse_ticks += ticks
    m.total_water_quantity_ticks += ticks
    m.cycles_rinse_clean_left += 1  # _right is DEAD


def simulate_cleaning(m: MachineState) -> None:
    """Full deep-clean — ~2770 ticks (~1 L), ~1 per day.

    2770 = 55425 total cleaning ticks / 20 deep cleans. Again, not the per-step
    median (136): a deep clean runs for minutes and its water lands across many
    samples, while the counter increments on only one of them. Trusting the
    per-step figure made cleaning water read 20x low, which would have shrunk
    the doughnut's largest slice to nothing.

    Note this does NOT touch cycles_rinse_clean_left: in the real data the
    rinse-cycle counter and the deep-clean counter move independently (144 vs
    20 over 24 days), so folding one into the other would triple the cleaning
    rate the dashboard reports.
    """
    ticks = random.randint(2400, 3100)
    m.total_water_cleaning_ticks += ticks
    m.total_water_quantity_ticks += ticks
    m.cleaning_counter += 1


def simulate_reset(m: MachineState) -> None:
    """Reproduce a real counter reset: every counter drops to zero at once.

    Happened 14 times in the 24-day capture. Any windowed MAX-MIN spanning this
    goes negative or spikes, so queries must guard against it.
    """
    for f in FIELDS:
        if f != "ts_ms":
            setattr(m, f, 0)


# ---------------------------------------------------------------------------
# CSV serialisation.
# THIS ORDER IS THE WIRE CONTRACT WITH THE REAL PI — 43 fields, no machine_id.
# It must stay byte-for-byte identical to the `logical:` schema block in every
# query YAML under scripts/coffe_queries/tcp/testv2/. Do not reorder or insert.
# ---------------------------------------------------------------------------

FIELDS = [
    "ts_ms",  # epoch SECONDS despite the name — see module docstring
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

assert len(FIELDS) == 43, f"real Pi sends exactly 43 fields, got {len(FIELDS)}"


def snapshot_to_csv_bytes(m: MachineState, all_zero: bool = False) -> bytes:
    """One wire row. `all_zero` reproduces the real dropout row (every counter
    zero but a live timestamp) — 35 of those appear in the 24-day capture."""
    values = []
    for f in FIELDS:
        if f == "ts_ms":
            values.append(now_epoch_seconds())
        else:
            values.append(0 if all_zero else getattr(m, f))
    buf = io.StringIO()
    # \r\n: the real Pi terminates rows with CRLF.
    csv.writer(buf, lineterminator="\r\n").writerow(values)
    return buf.getvalue().encode()


# ---------------------------------------------------------------------------
# Workload driver.
#
# The old model fired 0-3 brews EVERY tick — about 100x the real machine, which
# made short windows always look full. This drives products from the measured
# hour-of-day and weekday profile instead, so an idle 03:00 Sunday really is
# idle. That is the case the dashboard has to survive, so the mock must produce
# it rather than hide it.
# ---------------------------------------------------------------------------

def products_expected_this_tick(when: datetime, interval_sec: float, speedup: float) -> float:
    """Expected number of products in one tick, per the measured profile."""
    per_day = PRODUCTS_PER_DAY[when.weekday()]
    hour_share = HOURLY_PRODUCT_SHARE[when.hour]
    ticks_per_hour = 3600.0 / (interval_sec * speedup)
    return (per_day * hour_share) / ticks_per_hour


def advance_machine(
    m: MachineState,
    when: datetime,
    interval_sec: float,
    speedup: float,
    faults: bool,
) -> None:
    """Advance one tick of simulated time."""
    expected = products_expected_this_tick(when, interval_sec, speedup)
    # Poisson-ish: fractional expectation becomes a probability, so quiet hours
    # produce genuine gaps rather than a smeared trickle.
    n = int(expected) + (1 if random.random() < (expected % 1.0) else 0)
    for _ in range(n):
        if random.random() < NO_BREW_SHARE:
            kind = "nobrew"
        else:
            kind = "double" if random.random() < DOUBLE_SHARE else "single"
        simulate_brew(m, kind=kind)

    ticks_per_day = 86400.0 / (interval_sec * speedup)
    if random.random() < CLEANS_PER_DAY / ticks_per_day:
        simulate_cleaning(m)

    if faults:
        # Rates from the capture: 35 dropouts and 14 resets across 4839 rows.
        if random.random() < 35 / 4839:
            m.dropout_until = time.time() + interval_sec * random.randint(1, 3)
        elif random.random() < 14 / 4839:
            log.warning("fault injection: counter RESET (all counters -> 0)")
            simulate_reset(m)


def machine_snapshot_dict(m: MachineState) -> dict:
    d = {f: getattr(m, f) for f in FIELDS if f != "ts_ms"}
    d["ts_ms"] = now_epoch_seconds()
    return d


# ---------------------------------------------------------------------------
# TCP handler — NES connects here as a client and reads forever.
# ---------------------------------------------------------------------------

async def run_simulation(
    machine: MachineState,
    interval_sec: float,
    speedup: float,
    faults: bool,
) -> None:
    """The ONE authoritative clock for the machine.

    This must not live in the per-connection handler. The dashboard needs six
    queries running at once, and each one holds its own TCP connection; a
    per-connection loop would advance the same shared MachineState six times per
    tick, so every drink would be counted once per connected query. The bug is
    invisible with one client and silently sextuples every counter with six.

    Simulated time is what drives the workload profile, so --speedup compresses
    the hour-of-day curve too: a whole working day can play out in minutes while
    each individual event keeps its real measured size.
    """
    sim_time = datetime.now()
    while True:
        advance_machine(machine, sim_time, interval_sec, speedup, faults)
        await asyncio.sleep(interval_sec)
        sim_time += timedelta(seconds=interval_sec * speedup)


async def machine_handler(
    machine: MachineState,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    interval_sec: float,
) -> None:
    """Emit the current snapshot on an interval. Read-only — never mutates."""
    peer = writer.get_extra_info("peername")
    log.info("client connected from %s", peer)
    try:
        while True:
            dropped = time.time() < machine.dropout_until
            writer.write(snapshot_to_csv_bytes(machine, all_zero=dropped))
            await writer.drain()
            await asyncio.sleep(interval_sec)
    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        log.info("client %s disconnected", peer)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_machine_server(machine: MachineState, port: int, interval_sec: float) -> None:
    async def _handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await machine_handler(machine, r, w, interval_sec)

    # 0.0.0.0 so the NES worker inside the devcontainer can reach us via
    # host.docker.internal (a 127.0.0.1-only bind is not always reachable there).
    server = await asyncio.start_server(_handle, host="0.0.0.0", port=port)
    log.info("telemetry listening on 0.0.0.0:%d (real-Pi schema: 43 fields, epoch seconds, CRLF)", port)
    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# Admin HTTP server — minimal raw HTTP, no extra dependencies
# ---------------------------------------------------------------------------

async def admin_handler(
    machines: Dict[int, MachineState],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    interval_sec: float,
) -> None:
    try:
        request_line = (await asyncio.wait_for(reader.readline(), timeout=5.0)).decode().strip()
        if not request_line:
            return
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line in (b"\r\n", b"\n", b""):
                break

        parts = request_line.split()
        if len(parts) < 2:
            return
        method, path = parts[0], parts[1]

        query = ""
        if "?" in path:
            path, query = path.split("?", 1)
        params = dict(p.split("=", 1) for p in query.split("&") if "=" in p)

        if method == "GET" and path == "/status":
            json_response(writer, 200, {mid: machine_snapshot_dict(m) for mid, m in machines.items()})

        elif method == "GET" and path.startswith("/status/"):
            try:
                mid = int(path.split("/")[2])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"}); return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"}); return
            json_response(writer, 200, machine_snapshot_dict(m))

        elif method == "POST" and path.startswith("/simulate/brew/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"}); return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"}); return
            n = max(1, int(params.get("n", 1)))
            kind = params.get("kind", "single")
            milk = params.get("milk") in ("1", "true", "yes") if "milk" in params else None
            for _ in range(n):
                simulate_brew(m, kind=kind, milk=milk)
            log.info("admin: %d %s brew(s), milk=%s -> absolute_counter=%d",
                     n, kind, milk, m.absolute_counter)
            json_response(writer, 200, {"ok": True, "machineId": mid, "brews": n,
                                        "kind": kind, "milk": milk,
                                        "absolute_counter": m.absolute_counter,
                                        "total_coffee_bean_quantity_g": m.total_coffee_bean_quantity_g})

        elif method == "POST" and path.startswith("/simulate/rinse/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"}); return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"}); return
            simulate_rinse(m)
            log.info("admin: rinse -> cycles_rinse_clean_left=%d", m.cycles_rinse_clean_left)
            json_response(writer, 200, {"ok": True, "machineId": mid,
                                        "cycles_rinse_clean_left": m.cycles_rinse_clean_left})

        elif method == "POST" and path.startswith("/simulate/clean/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"}); return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"}); return
            simulate_cleaning(m)
            log.info("admin: cleaning -> cleaning_counter=%d", m.cleaning_counter)
            json_response(writer, 200, {"ok": True, "machineId": mid,
                                        "cleaning_counter": m.cleaning_counter})

        elif method == "POST" and path.startswith("/simulate/reset/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"}); return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"}); return
            simulate_reset(m)
            log.warning("admin: RESET — every counter zeroed")
            json_response(writer, 200, {"ok": True, "machineId": mid, "reset": True})

        elif method == "POST" and path.startswith("/simulate/dropout/"):
            try:
                mid = int(path.split("/")[3])
            except (IndexError, ValueError):
                json_response(writer, 400, {"error": "invalid machineId"}); return
            m = machines.get(mid)
            if m is None:
                json_response(writer, 404, {"error": f"machine {mid} not found"}); return
            n = max(1, int(params.get("n", 1)))
            m.dropout_until = time.time() + interval_sec * n
            log.warning("admin: all-zero dropout for the next %d row(s), all clients", n)
            json_response(writer, 200, {"ok": True, "machineId": mid, "dropoutRows": n})
        else:
            json_response(writer, 404, {"error": "unknown route"})

    except asyncio.TimeoutError:
        pass
    except Exception as e:
        log.warning("admin handler error: %s", e)
    finally:
        try:
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_admin_server(machines: Dict[int, MachineState], port: int,
                           interval_sec: float) -> None:
    async def _handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        await admin_handler(machines, r, w, interval_sec)

    server = await asyncio.start_server(_handle, host="0.0.0.0", port=port)
    log.info("admin HTTP listening on 0.0.0.0:%d", port)
    log.info("  curl -X POST http://localhost:%d/simulate/brew/101", port)
    log.info("  curl -X POST http://localhost:%d/simulate/clean/101", port)
    async with server:
        await server.serve_forever()


# ---------------------------------------------------------------------------
# Entry point. ONE machine on ONE port — the real Pi is a single machine on
# 2222. Unlike the real Pi, this accepts unlimited concurrent clients, so every
# NES query can hold its own connection at the same time.
# ---------------------------------------------------------------------------

MACHINE_ID = 101


async def main(port: int, interval_sec: float, admin_port: int,
               speedup: float, faults: bool) -> None:
    machines: Dict[int, MachineState] = {MACHINE_ID: MachineState(machine_id=MACHINE_ID)}
    log.info("Eversys telemetry server — REAL-PI SCHEMA (43 fields, no machine_id)")
    log.info("emit interval: %.1fs | speedup: %.1fx | fault injection: %s",
             interval_sec, speedup, "on" if faults else "off")
    log.info("workload calibrated to 24 days of real data: ~%d products/weekday, "
             "06:00-17:00 UTC, peak 11:00", PRODUCTS_PER_DAY[0])
    if speedup == 1.0:
        log.info("running at REAL machine speed — expect long idle stretches; "
                 "use --speedup 60 or the admin API to drive a demo")
    await asyncio.gather(
        asyncio.create_task(run_simulation(machines[MACHINE_ID], interval_sec, speedup, faults)),
        asyncio.create_task(run_machine_server(machines[MACHINE_ID], port, interval_sec)),
        asyncio.create_task(run_admin_server(machines, admin_port, interval_sec)),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Eversys TCP telemetry server — real-Pi schema")
    parser.add_argument("--interval", type=float, default=8.0,
                        help="seconds between snapshots; 8.0 is the real Pi's "
                             "measured median (default: 8.0)")
    parser.add_argument("--speedup", type=float, default=1.0,
                        help="compress simulated time so a working day plays out "
                             "in minutes. Event sizes stay real; only the rate and "
                             "the hour-of-day curve speed up (default: 1.0 = real time)")
    parser.add_argument("--no-faults", action="store_true",
                        help="disable injection of all-zero dropout rows and counter "
                             "resets. Both occur on the real machine; leave them on "
                             "unless you are debugging something else")
    parser.add_argument("--port", type=int, default=2222,
                        help="telemetry TCP port; matches the SSH tunnel so query YAMLs "
                             "need no changes when swapping to the real Pi (default: 2222)")
    parser.add_argument("--admin-port", type=int, default=8000,
                        help="admin HTTP API port (default: 8000)")
    args = parser.parse_args()
    try:
        asyncio.run(main(port=args.port, interval_sec=args.interval,
                         admin_port=args.admin_port, speedup=args.speedup,
                         faults=not args.no_faults))
    except KeyboardInterrupt:
        log.info("shutting down")
