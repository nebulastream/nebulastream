# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
import math

def udf(x):
    try:
        v = math.pow(x, 0.5)
        r = str(int(v * 1000) if math.isfinite(v) else -1)
    except ValueError:
        r = "dom"
    try:
        w = math.pow(x, 1e300)
        r += ",inf" if math.isinf(w) else ",fin"
    except OverflowError:
        r += ",ovf"
    return r
