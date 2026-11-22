# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
import math

def udf(x):
    r = []
    try:
        r.append(f"{math.exp(x):.3e}")
    except OverflowError:
        r.append("ovf")
    try:
        r.append(f"{x ** 10:.3e}")
    except OverflowError:
        r.append("ovf")
    try:
        r.append(f"{math.sqrt(x):.3e}")
    except ValueError:
        r.append("dom")
    try:
        r.append(f"{math.log(x):.3e}")
    except ValueError:
        r.append("dom")
    return ",".join(r)
