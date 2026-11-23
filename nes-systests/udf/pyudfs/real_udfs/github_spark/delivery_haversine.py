# codon-fuzz: entry=haversine args=FLOAT64,FLOAT64,FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[12.9716, 77.5946, 12.9352, 77.6245], [12.9279, 77.6271, 12.9784, 77.6408], [28.6139, 77.209, 28.5355, 77.391], [19.076, 72.8777, 19.076, 72.8777], [13.0827, 80.2707, 12.9165, 79.1325], [22.5726, 88.3639, 22.5958, 88.2636]]
# real-udf: origin=github-spark id=delivery_haversine requires= wrapped=0 ported=0
# Source: https://github.com/KhalifaSeck/realtime-delivery-lakehouse/blob/b83ca2607a9563842a92481234b44078439f9c7f/streaming/jobs/metrics.py#L43-L55
# Repository: KhalifaSeck/realtime-delivery-lakehouse @ commit b83ca2607a9563842a92481234b44078439f9c7f
# License: MIT
# Copied verbatim from streaming/jobs/metrics.py (L29-L29, L43-L55); only the module's own import lines that the code uses are prepended.

import math


_EARTH_RADIUS_KM = 6371.0


def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return _EARTH_RADIUS_KM * c
