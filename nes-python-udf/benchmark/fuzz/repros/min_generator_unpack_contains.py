# codon-fuzz: entry=udf args=INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[3], [0], [7]]
# Before Codon fork 3b8c3a9+, unpacking a generator (a, b = map(...)), `x in d.keys()`, `x in (generator)` and a bare
# lambda statement did not compile (found in the real UDFs traffic_haversine, vim89_udf_lookups and the Babelfish UDFs).
import math


def g(a, ctx):
    return a + 1

lambda a, ctx: g(a, ctx)


def udf(n):
    a, b = map(int, [str(n), "7"])
    lat, lon = map(math.radians, [float(n), 2.0])
    d = {3: 1}
    return a + b + (1 if n in d.keys() else 0) + (1 if n in (x * 3 for x in range(3)) else 0) + int(math.atan2(lat, lon) * 100)
