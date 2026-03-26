import time
import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Optional, List

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Eversys Telemetry Dummy (Stateful)")

# -----------------------------
# Helpers
# -----------------------------


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def iso_local_now_naive() -> str:
    # The examples you pasted use no "Z" suffix for machineTimestamp and lastReset.
    # We'll mimic that by returning a naive ISO string.
    return datetime.now().replace(microsecond=0).isoformat()


def bad_request(message: str = "Bad request", status: int = 400):
    # Matches your example shape (message/status/timeStamp)
    return JSONResponse(
        status_code=400,
        content={"message": message, "status": status, "timeStamp": now_ms()},
    )


# -----------------------------
# Domain state
# -----------------------------


@dataclass
class MachineState:
    machine_id: int
    group_id: int

    # Counters (monotonic)
    absoluteCounter: int = 0
    cleaningCounter: int = 0

    brew_left: int = 0
    brew_right: int = 0

    # Quantities / times (float-ish, internal)
    beans_total_kg: float = 0.0
    water_products_l: float = 0.0
    water_total_l: float = 0.0

    air_pump_time_s: float = 0.0
    hot_water_time_s: float = 0.0

    heating_time_coffee_left_s: float = 0.0
    heating_time_coffee_right_s: float = 0.0

    milk_cycles_left: int = 0
    milk_cycles_right: int = 0
    milk_total_time_s: float = 0.0
    milk_steam_left_s: float = 0.0
    milk_steam_right_s: float = 0.0

    steam_time_left_s: float = 0.0
    steam_time_right_s: float = 0.0
    heating_time_steam_boilers_s: List[float] = field(default_factory=lambda: [0.0])

    water_cleaning_l: float = 0.0
    water_purge_l: float = 0.0
    water_rinse_l: float = 0.0

    powder_time_s: float = 0.0
    powder_total_left_kg: float = 0.0
    powder_total_right_kg: float = 0.0

    rinse_clean_left: int = 0
    rinse_clean_right: int = 0

    # Maintenance
    service_interval_products: int = 3000
    service_due_counter: int = 3000  # countdown
    water_filter_capacity_l: int = 2000
    water_filter_due_l: int = 2000  # countdown

    # Cleaning ratings windows (we store last 30 days as timestamps of cleanings)
    cleaning_events_ms: List[int] = field(default_factory=list)
    cleaning_without_detergent_ms: List[int] = field(default_factory=list)
    cleaning_without_everclean_ms: List[int] = field(default_factory=list)
    cleaning_without_tabs_ms: List[int] = field(default_factory=list)

    lastReset: str = field(default_factory=iso_local_now_naive)
    lastUpdate: str = field(default_factory=iso_local_now_naive)

    # Auto-brew loop control
    auto_task: Optional[asyncio.Task] = None
    auto_interval_sec: float = 0.0


# One group: Dima office
GROUPS = {1: {"name": "dima_office", "machines": [101, 102, 103]}}

# Machine registry
MACHINES: Dict[int, MachineState] = {
    mid: MachineState(machine_id=mid, group_id=1) for mid in GROUPS[1]["machines"]
}

# Protect state mutations (single process; adequate for demo)
STATE_LOCK = asyncio.Lock()

# -----------------------------
# Simulation logic
# -----------------------------


async def simulate_brew(machine: MachineState):
    """
    One 'brew' changes multiple counters in a correlated, realistic way.
    """
    # choose side
    side = random.choice(["left", "right"])

    # plausible ranges (tweak freely)
    beans_g = random.uniform(7.0, 11.0)  # grams per coffee
    water_ml = random.uniform(30.0, 120.0)  # ml per drink
    air_pump_s = random.uniform(0.3, 1.5)
    brew_heat_s = random.uniform(2.0, 8.0)

    # milk drink? (20% chance)
    milk = random.random() < 0.2
    milk_time_s = random.uniform(3.0, 10.0) if milk else 0.0
    steam_s = random.uniform(1.0, 6.0) if milk else 0.0

    machine.absoluteCounter += 1
    if side == "left":
        machine.brew_left += 1
        machine.heating_time_coffee_left_s += brew_heat_s
    else:
        machine.brew_right += 1
        machine.heating_time_coffee_right_s += brew_heat_s

    machine.beans_total_kg += beans_g / 1000.0
    machine.water_products_l += water_ml / 1000.0
    machine.water_total_l += water_ml / 1000.0

    machine.air_pump_time_s += air_pump_s

    if milk:
        if side == "left":
            machine.milk_cycles_left += 1
            machine.milk_steam_left_s += steam_s
        else:
            machine.milk_cycles_right += 1
            machine.milk_steam_right_s += steam_s

        machine.milk_total_time_s += milk_time_s
        if side == "left":
            machine.steam_time_left_s += steam_s
        else:
            machine.steam_time_right_s += steam_s
        # keep a single boiler time entry for simplicity
        machine.heating_time_steam_boilers_s[0] += random.uniform(0.5, 2.0)

    # Maintenance countdowns (simple model)
    machine.service_due_counter = max(0, machine.service_due_counter - 1)
    machine.water_filter_due_l = max(0, machine.water_filter_due_l - int(water_ml / 10))  # rough

    machine.lastUpdate = iso_local_now_naive()


async def auto_brew_loop(machine_id: int):
    """
    Background task: brews every N seconds until stopped.
    """
    while True:
        async with STATE_LOCK:
            m = MACHINES.get(machine_id)
            if m is None:
                return
            interval = m.auto_interval_sec

        # brew
        async with STATE_LOCK:
            m = MACHINES.get(machine_id)
            if m is None:
                return
            await simulate_brew(m)

        await asyncio.sleep(max(0.1, interval))


def cleanup_old_cleanings(ms_list: List[int], window_days: int) -> List[int]:
    cutoff = now_ms() - int(window_days * 24 * 3600 * 1000)
    return [t for t in ms_list if t >= cutoff]


# -----------------------------
# Control endpoints (simulation)
# -----------------------------


@app.post("/simulate/brew/{machineId}")
async def api_simulate_brew(machineId: int):
    async with STATE_LOCK:
        m = MACHINES.get(machineId)
        if not m:
            return bad_request("Unknown machineId")
        await simulate_brew(m)
        return {"ok": True, "machineId": machineId, "serverTimestamp": iso_utc_now()}


@app.post("/simulate/start/{machineId}")
async def api_simulate_start(machineId: int, interval_sec: float = 5.0):
    async with STATE_LOCK:
        m = MACHINES.get(machineId)
        if not m:
            return bad_request("Unknown machineId")
        if m.auto_task and not m.auto_task.done():
            return {"ok": True, "running": True, "interval_sec": m.auto_interval_sec}

        m.auto_interval_sec = float(interval_sec)
        m.auto_task = asyncio.create_task(auto_brew_loop(machineId))
        return {"ok": True, "running": True, "interval_sec": m.auto_interval_sec}


@app.post("/simulate/stop/{machineId}")
async def api_simulate_stop(machineId: int):
    async with STATE_LOCK:
        m = MACHINES.get(machineId)
        if not m:
            return bad_request("Unknown machineId")
        if m.auto_task and not m.auto_task.done():
            m.auto_task.cancel()
        m.auto_task = None
        m.auto_interval_sec = 0.0
        return {"ok": True, "running": False}


# -----------------------------
# The 4 selected API endpoints
# -----------------------------


# @app.get("/v3/machines/machine-counters/{machineId}")
# async def machine_counters(machineId: int):
#     async with STATE_LOCK:
#         m = MACHINES.get(machineId)
#         if not m:
#             return bad_request("Unknown machineId")

#         # float-like values
#         water_cleaning = round(m.water_cleaning_l, 3)
#         water_products = round(m.water_products_l, 3)
#         water_purge = round(m.water_purge_l, 3)
#         water_rinse = round(m.water_rinse_l, 3)
#         water_total = round(m.water_total_l, 3)

#         return {
#             "MACHINEID": m.machine_id,
#             "WATER": {
#                 "CLEANINGQUANTITY": water_cleaning,
#                 "PRODUCTSQUANTITY": water_products,
#                 "PURGEQUANTITY": water_purge,
#                 "RINSEQUANTITY": water_rinse,
#                 "TOTALQUANTITY": water_total,
#             },
#         }

@app.get("/v3/machines/machine-counters/{machineId}")
async def machine_counters(machineId: int):
    async with STATE_LOCK:
        m = MACHINES.get(machineId)
        if not m:
            return bad_request("Unknown machineId")

        return {
            "ABSOLUTECOUNTER": m.absoluteCounter,
            "AIRPUMPTIME": round(m.air_pump_time_s, 3),
            "BEANS": {
                "ADJUSTMENT": 0,
                "FRONTORRIGHTQUANTITY": round(m.beans_total_kg * 0.25, 4),
                "POWDERCHUTEQUANTITY": round(m.beans_total_kg * 0.05, 4),
                "QUANTITYHOPPER1": round(m.beans_total_kg * 0.25, 4),
                "QUANTITYHOPPER2": round(m.beans_total_kg * 0.25, 4),
                "QUANTITYHOPPER3": round(m.beans_total_kg * 0.125, 4),
                "QUANTITYHOPPER4": round(m.beans_total_kg * 0.125, 4),
                "REARORLEFTQUANTITY": round(m.beans_total_kg * 0.25, 4),
                "TOTALQUANTITY": round(m.beans_total_kg, 4),
            },
            "BREWCYCLES": {
                "LEFT": m.brew_left,
                "RIGHT": m.brew_right,
            },
            "CLEANINGCOUNTER": m.cleaningCounter,
            "HEATINGTIMECOFFEEBOILER": {
                "LEFT": round(m.heating_time_coffee_left_s, 3),
                "RIGHT": round(m.heating_time_coffee_right_s, 3),
            },
            "HOTWATERTIME": round(m.hot_water_time_s, 3),
            "LASTRESET": m.lastReset,
            "MACHINEID": m.machine_id,
            "MACHINETIMESTAMP": iso_local_now_naive(),
            "MILK": {
                "CYCLES": {
                    "LEFT": m.milk_cycles_left,
                    "RIGHT": m.milk_cycles_right,
                },
                "STEAMTIME": {
                    "LEFT": round(m.milk_steam_left_s, 3),
                    "RIGHT": round(m.milk_steam_right_s, 3),
                },
                "TOTALTIME": round(m.milk_total_time_s, 3),
            },
            "POWDER": {
                "TIME": round(m.powder_time_s, 3),
                "TOTALQUANTITYLEFT": round(m.powder_total_left_kg, 4),
                "TOTALQUANTITYRIGHT": round(m.powder_total_right_kg, 4),
            },
            "RINSEORCLEANCYCLES": {
                "LEFT": m.rinse_clean_left,
                "RIGHT": m.rinse_clean_right,
            },
            "SERVERTIMESTAMP": iso_utc_now(),
            "STEAM": {
                "HEATINGTIMESTEAMBOILERS": [
                    round(x, 3) for x in m.heating_time_steam_boilers_s
                ],
                "STEAMTIME": {
                    "LEFT": round(m.steam_time_left_s, 3),
                    "RIGHT": round(m.steam_time_right_s, 3),
                },
            },
            "WATER": {
                "CLEANINGQUANTITY": round(m.water_cleaning_l, 3),
                "PRODUCTSQUANTITY": round(m.water_products_l, 3),
                "PURGEQUANTITY": round(m.water_purge_l, 3),
                "RINSEQUANTITY": round(m.water_rinse_l, 3),
                "TOTALQUANTITY": round(m.water_total_l, 3),
            },
        }


@app.get("/v3/machines/maintenance-data/{machineId}")
async def maintenance_data(machineId: int):
    async with STATE_LOCK:
        m = MACHINES.get(machineId)
        if not m:
            return bad_request("Unknown machineId")

        # Derive due dates (simple: today + countdown)
        # In real life you'd base on last service date and interval; this is enough for a simulator.
        due_service = (date.today() + timedelta(days=max(0, m.service_due_counter // 50))).isoformat()
        due_filter = (date.today() + timedelta(days=max(0, m.water_filter_due_l // 200))).isoformat()

        return {
            "lastUpdate": m.lastUpdate,
            "machineId": m.machine_id,
            "serverTimestamp": iso_utc_now(),
            "serviceCountdown": m.service_due_counter,
            "serviceDateInterval": 0,
            "serviceDueDate": due_service,
            "serviceInterval": m.service_interval_products,
            "waterFilterCapacity": m.water_filter_capacity_l,
            "waterFilterCountdown": m.water_filter_due_l,
            "waterFilterDateInterval": 0,
            "waterFilterDueDate": due_filter,
        }


@app.get("/v3/machines/cleaning-ratings/{machineId}")
async def cleaning_ratings(machineId: int):
    async with STATE_LOCK:
        m = MACHINES.get(machineId)
        if not m:
            return bad_request("Unknown machineId")

        # Keep windows clean
        m.cleaning_events_ms = cleanup_old_cleanings(m.cleaning_events_ms, 30)
        m.cleaning_without_detergent_ms = cleanup_old_cleanings(
            m.cleaning_without_detergent_ms, 30
        )
        m.cleaning_without_everclean_ms = cleanup_old_cleanings(
            m.cleaning_without_everclean_ms, 30
        )
        m.cleaning_without_tabs_ms = cleanup_old_cleanings(m.cleaning_without_tabs_ms, 30)

        last7 = cleanup_old_cleanings(m.cleaning_events_ms, 7)
        last30 = m.cleaning_events_ms

        # A simple rating model:
        # - good if >= 7 cleanings/30d
        # - warning if < 7
        n30 = len(last30)
        if n30 >= 7:
            status = "OKAY"
            rating = 90
        elif n30 >= 3:
            status = "WARNING"
            rating = 60
        else:
            status = "CRITICAL"
            rating = 20

        # Split counts for "without X" (we simulate none by default, but the fields exist)
        last7_wo_det = len(cleanup_old_cleanings(m.cleaning_without_detergent_ms, 7))
        last30_wo_det = len(m.cleaning_without_detergent_ms)

        last7_wo_ever = len(cleanup_old_cleanings(m.cleaning_without_everclean_ms, 7))
        last30_wo_ever = len(m.cleaning_without_everclean_ms)

        last7_wo_tabs = len(cleanup_old_cleanings(m.cleaning_without_tabs_ms, 7))
        last30_wo_tabs = len(m.cleaning_without_tabs_ms)

        return {
            "cleaningRating": rating,
            "cleaningStatus": status,
            "cleaningsLast30Days": len(last30),
            "cleaningsLast30DaysWithoutDetergent": last30_wo_det,
            "cleaningsLast30DaysWithoutEverclean": last30_wo_ever,
            "cleaningsLast30DaysWithoutTabs": last30_wo_tabs,
            "cleaningsLast7Days": len(last7),
            "cleaningsLast7DaysWithoutDetergent": last7_wo_det,
            "cleaningsLast7DaysWithoutEverclean": last7_wo_ever,
            "cleaningsLast7DaysWithoutTabs": last7_wo_tabs,
            "machineId": m.machine_id,
            "serverTimestamp": iso_utc_now(),
        }


@app.get("/v3/dashboards/groups/statistic-views/{groupId}")
async def group_statistics(groupId: int):
    group = GROUPS.get(groupId)
    if not group:
        return bad_request("Unknown groupId")

    async with STATE_LOCK:
        machine_ids = group["machines"]
        total_today = 0
        total_this_week = 0
        total_yesterday = 0
        total_last_week = 0

        # Simple aggregation model:
        # We do not store per-day history yet (v2 can), so we fake a split.
        # We use absoluteCounter as the overall usage signal and distribute it.
        for mid in machine_ids:
            m = MACHINES[mid]
            c = m.absoluteCounter
            # distribute deterministically-ish
            today = int(c * 0.10)
            yesterday = int(c * 0.07)
            this_week = int(c * 0.35)
            last_week = int(c * 0.25)

            total_today += today
            total_yesterday += yesterday
            total_this_week += this_week
            total_last_week += last_week

        return {
            "serverTimestamp": iso_utc_now(),
            "totalProductsLastWeek": total_last_week,
            "totalProductsThisWeek": total_this_week,
            "totalProductsToday": total_today,
            "totalProductsYesterday": total_yesterday,
        }


# Optional: a tiny helper to list machines/groups (not part of your 4 endpoints, but handy)
@app.get("/simulate/info")
async def simulate_info():
    return {
        "groups": GROUPS,
        "machines": sorted(list(MACHINES.keys())),
        "note": "groupId=1 represents dima_office",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app2:app", host="0.0.0.0", port=8000, reload=True)
