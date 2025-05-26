import csv
import random
import datetime
import numpy as np

def generate_iot_dataset(num_records, output_file):
    """     Generate IoT sensor dataset with characteristics that benefit from columnar layout:
       - High cardinality numeric columns for aggregation
       - Selective filters on specific columns
            - Wide schema with many unused columns
                 """
    # Start time
    base_time = datetime.datetime(2024, 1, 1)
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        # Header - 20 columns to make row->col transformation worthwhile
        headers = [
            'sensor_id', 'location_id', 'timestamp',
            'temperature', 'humidity', 'pressure', 'light_level',
            'noise_level', 'air_quality', 'co2_level',
            'power_consumption', 'battery_level', 'signal_strength',
            'error_count', 'maintenance_flag', 'calibration_offset',
            'avg_daily_temp', 'max_daily_temp', 'min_daily_temp', 'status_code'
        ]
        writer.writerow(headers)
        # Generate data with patterns that benefit columnar processing
        for i in range(num_records):
            sensor_id = i % 10000  # 10k unique sensors
            location_id = i % 100  # 100 locations
            timestamp = int((base_time + datetime.timedelta(seconds=i)).timestamp())
            # Correlated values for realistic patterns
            base_temp = 20 + (location_id % 10) * 2
            temperature = round(base_temp + random.gauss(0, 2), 2)
            humidity = round(max(0, min(100, 60 + (temperature - 20) * -2 + random.gauss(0, 5))), 2)
            pressure = round(1013.25 + random.gauss(0, 10), 2)
            # Values with different distributions
            light_level = int(random.lognormvariate(5, 1.5))
            noise_level = round(random.uniform(30, 90), 2)
            air_quality = int(random.choice([50, 100, 150, 200, 300]) + random.randint(-20, 20))
            co2_level = int(400 + random.lognormvariate(3, 0.5))
            # Power and battery with patterns
            power_consumption = round(random.uniform(0.5, 5.0), 3)
            battery_level = round(max(0, min(100, 100 - (i % 1000) * 0.1)), 2)
            signal_strength = int(random.choice([-90, -80, -70, -60, -50]) + random.randint(-5, 5))
            # Sparse columns
            error_count = 0 if random.random() > 0.05 else random.randint(1, 10)
            maintenance_flag = 0 if random.random() > 0.01 else 1
            calibration_offset = 0.0 if random.random() > 0.1 else round(random.uniform(-0.5, 0.5), 3)
            # Aggregated values
            avg_daily_temp = round(base_temp + random.gauss(0, 0.5), 2)
            max_daily_temp = round(avg_daily_temp + random.uniform(3, 8), 2)
            min_daily_temp = round(avg_daily_temp - random.uniform(3, 8), 2)
            status_code = random.choice([200, 200, 200, 200, 201, 202, 400, 404, 500])
            writer.writerow([
                sensor_id, location_id, timestamp,
                temperature, humidity, pressure, light_level,
                noise_level, air_quality, co2_level,
                power_consumption, battery_level, signal_strength,
                error_count, maintenance_flag, calibration_offset,
                avg_daily_temp, max_daily_temp, min_daily_temp, status_code
            ])

# Generate datasets
path = "/home/user/CLionProjects/nebulastream-public/cmake-build-dockerdebugporf/nes-systests/testdata/large/iot/"
generate_iot_dataset(10_000_000, path + 'iot_sensors_10m.csv')  # ~2GB
generate_iot_dataset(50_000_000, path + 'iot_sensors_50m.csv')  # ~10GB