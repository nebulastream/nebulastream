# codon-fuzz: entry=haversine_distance args=FLOAT64,FLOAT64,FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[30.132812, -97.641144, 30.133011, -97.640821], [30.133011, -97.640821, 30.133011, -97.640821], [30.267153, -97.743057, 29.760427, -95.369804], [30.13442, -97.63891, 30.13461, -97.63855], [52.0715, -1.0167, 52.0786, -1.0153], [-33.8568, 151.2153, 51.5072, -0.1276]]
# real-udf: origin=streaming id=lhre_track_mapper_geo requires= wrapped=0 ported=0
# Imports: the original module's imports that this code uses.

import math

# Source: https://github.com/LonghornRacingElectric/lhre/blob/85932a3964a680752f3ba6e2538dd7bab5544419/apps/telemetry/stack/processors/track_mapper/main.py#L55-L100
# Repository: https://github.com/LonghornRacingElectric/lhre (commit 85932a3964a680752f3ba6e2538dd7bab5544419)
# License: MIT
# System: Kafka consumer (plain Python)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

class SimpleKalmanFilter:
    """
    Minimizes GPS jitter by keeping an estimate of position and uncertainty.
    """
    def __init__(self, process_noise=1e-4, measurement_noise=1e-2, estimation_error=1.0):
        self.q = process_noise       # Process noise covariance
        self.r = measurement_noise   # Measurement noise covariance
        self.p = estimation_error    # Estimation error covariance
        self.x = 0.0                 # Value (Lat or Lon)

    def update(self, measurement):
        # Prediction update
        self.p = self.p + self.q

        # Measurement update
        k = self.p / (self.p + self.r) # Kalman gain
        self.x = self.x + k * (measurement - self.x)
        self.p = (1 - k) * self.p
        return self.x


def haversine_distance(lat1, lon1, lat2, lon2):
    # Earth's mean radius in meters
    R = 6371000  

    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def is_intersection(line1: tuple[tuple[float, float], tuple[float, float]], line2: tuple[tuple[float, float], tuple[float, float]]) -> bool:
    """
    Checks if two line segments intersect. Used for lap detection with the gate.
    """
    denominator = (line1[0][1] - line1[1][1]) * (line2[0][0] - line2[1][0]) - (line1[0][0] - line1[1][0]) * (line2[0][1] - line2[1][1])
    if denominator == 0:
        return False
    t = ((line1[0][1] - line2[0][1]) * (line2[0][0] - line2[1][0]) - (line1[0][0] - line2[0][0]) * (line2[0][1] - line2[1][1])) / denominator
    u = ((line1[0][1] - line2[0][1]) * (line1[0][0] - line1[1][0]) - (line1[0][0] - line2[0][0]) * (line1[0][1] - line1[1][1])) / denominator
    return 0 <= t <= 1 and 0 <= u <= 1
