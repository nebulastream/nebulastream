# codon-fuzz: entry=haversine args=FLOAT64,FLOAT64,FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[51.5074, -0.1278, 48.8566, 2.3522], [40.7128, -74.006, 34.0522, -118.2437], [52.52, 13.405, 52.52, 13.405], [-33.8688, 151.2093, -37.8136, 144.9631], [35.6762, 139.6503, 37.5665, 126.978], [0.0, 0.0, 0.0, 180.0]]
# real-udf: origin=github-spark id=traffic_haversine requires= wrapped=0 ported=0
# Source: https://github.com/aleksandarbrkljac/traffic-monitoring/blob/8183addc2d7b991ed4637ba66803be2cf79423c2/streaming/consumer.py#L20-L47
# Repository: aleksandarbrkljac/traffic-monitoring @ commit 8183addc2d7b991ed4637ba66803be2cf79423c2
# License: MIT
# Copied verbatim from streaming/consumer.py (L20-L47); only the module's own import lines that the code uses are prepended.

import math


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points on earth given by their coordinates in degrees using the Haversine formula.

    Args:
        lat1 (float): Latitude of the first coordinate.
        lon1 (float): Longitude of the first coordinate.
        lat2 (float): Latitude of the second coordinate.
        lon2 (float): Longitude of the second coordinate.


    Returns:
        float: Distance in kilometers.
    """
    # Convert coordinates to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [float(
        lat1), float(lon1), float(lat2), float(lon2)])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    # Earth's radius in kilometers
    r = 6371
    return c * r
