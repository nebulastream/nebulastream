# codon-fuzz: entry=case_entry args=FLOAT64,FLOAT64,FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[40.7128, -74.006, 34.0522, -118.2437], [52.52, 13.405, 48.8566, 2.3522], [40.76727216, -73.99392888, 40.70563889, -74.01017344], [51.5074, -0.1278, 51.5074, -0.1278], [-33.8688, 151.2093, 35.6762, 139.6503], [37.7749, -122.4194, 37.8044, -122.2712]]
# real-udf: origin=literature id=babelfish_haversine_lib requires=haversine wrapped=1 ported=0
# Wrapper: Babelfish passes a record object with typed fields; case_entry builds it from scalar arguments.
# Wrapper: the source lambda calls udf(rec) with one argument, which raises TypeError; case_entry calls udf(rec, None).

# Source: Babelfish: Efficient Execution of Polyglot Queries (Grulich, Zeuch, Markl, PVLDB 2021/2022)
# URL: https://github.com/TU-Berlin-DIMA/babelfish/blob/4926384d994ab4ab324a056d69be64d9b52ed7a0/benchmark/src/main/java/de/tub/dima/babelfish/benchmark/externalLibs/Queries.java#L222
# Repository: https://github.com/TU-Berlin-DIMA/babelfish at commit 4926384d994ab4ab324a056d69be64d9b52ed7a0
# License: Apache-2.0
# Path: benchmark/src/main/java/de/tub/dima/babelfish/benchmark/externalLibs/Queries.java, lines 222-222
# Note: decoded verbatim from a Java string literal (escape sequences resolved).

import site
from haversine import haversine, Unit

def udf(rec,ctx):
	start = (rec.start_lat, rec.start_lon)
	end = (rec.end_lat, rec.end_lon)
	distance = haversine(start, end)
	return distance

lambda rec,ctx: udf(rec)


class Rec:
    def __init__(self, start_lat: float, start_lon: float, end_lat: float, end_lon: float):
        self.start_lat = start_lat
        self.start_lon = start_lon
        self.end_lat = end_lat
        self.end_lon = end_lon


def case_entry(start_lat, start_lon, end_lat, end_lon):
    return udf(Rec(start_lat, start_lon, end_lat, end_lon), None)
