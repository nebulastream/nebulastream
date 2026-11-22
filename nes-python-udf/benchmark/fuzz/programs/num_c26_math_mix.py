# codon-fuzz: entry=udf args=FLOAT64,INT64 returns=FLOAT64
import math

def udf(x, n):
    m = n % 50
    acc = math.fsum([x, 1e16, -1e16]) if math.isfinite(x) else 0.0
    acc += math.prod([1.5, 2.0, float(m)])
    acc += float(math.isqrt(abs(n) % 10000))
    acc += math.remainder(float(m), 3.0)
    return acc
