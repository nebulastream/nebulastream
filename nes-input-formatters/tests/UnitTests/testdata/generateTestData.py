import random
import string
import csv
from datetime import datetime, timedelta

# Define possible values for the varchar field (spacecraft states)
STATES = [
    "NOMINAL", "SAFE_MODE", "CHARGING", "TRANSMITTING",
    "SCIENCE_OPS", "MAINTENANCE", "ATTITUDE_ADJUSTMENT",
    "TRAJECTORY_CORRECTION", "STANDBY", "CALIBRATING"
]

def generate_telemetry_data():
    data = []
    start_time = datetime.now()

    for i in range(1000):
        # temperature_delta: signed int (-50 to 50 Celsius)
        temp_delta = random.randint(-50, 50)

        # power_level: unsigned int (0 to 1000 watts)
        power = random.randint(0, 1000)

        # is_sunlit: boolean
        is_sunlit = random.choice([1, 0])

        # status_code: single character (A-Z)
        status = random.choice(string.ascii_uppercase)

        # operation_state: varchar(40)
        state = random.choice(STATES)

        # radiation_level: float (0.0 to 100.0 rads)
        radiation = round(random.uniform(0, 100), 2)

        # orbital_velocity: double (7.0 to 8.0 km/s)
        velocity = round(random.uniform(7.0, 8.0), 6)

        data.append([
            temp_delta,
            power,
            is_sunlit,
            status,
            state,
            radiation,
            velocity
        ])

    return data

# Generate and write data to CSV
header = [
    "temperature_delta",
    "power_level",
    "is_sunlit",
    "status_code",
    "operation_state",
    "radiation_level",
    "orbital_velocity"
]

data = generate_telemetry_data()

filename = 'Spacecraft_Telemetry_1000_Lines.csv'

with open(filename, 'w', newline='\n') as f:
    writer = csv.writer(f, delimiter='|')
    writer.writerows(data)

with open(filename, 'rb') as f:
    content = f.read()

# Replace \r\n (Windows) and \r (Mac) with \n
normalized = content.replace(b'\r\n', b'\n').replace(b'\r', b'\n')

with open(filename, 'wb') as f:
    f.write(normalized)
