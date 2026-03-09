types = ["wind_turbine", "solar_panel", "hydro_plant"]
statuses = ["produced", "idle", "fault"]

i = 0
while i < 3:
    status = statuses[i]
    value = i*100
    t = types[i]
    # CSV: id,status,value,type
    print(f"{i},{status},{value},{t}", flush=True)
    i += 1