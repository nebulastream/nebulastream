import json


data = [
    {"id": 0, "status": "produced", "value": 0, "source": "wind_turbine"},
    {"id": 1, "status": "idle", "value": 100, "source": "solar_panel"},
    {"id": 2, "status": "fault", "value": 200, "source": "hydro_plant"}
]


print(json.dumps(data, indent=2))