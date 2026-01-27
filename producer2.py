import random

types = ["wind_turbine", "solar_panel", "hydro_plant"]
statuses = ["produced", "idle", "fault"]

i = 1
while i < 10:
    status = random.choice(statuses)
    value = random.randint(1, 10)
    t = random.choice(types)
    # CSV: id,status,value,type
    print(f"{i},{status},{value},{t}", flush=True)
    i += 1