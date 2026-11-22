# codon-fuzz: entry=udf args=FLOAT64,INT64 returns=VARSIZED
def udf(x, n):
    out = []
    out.append(str(round(x)) if x == x and abs(x) < 1e300 else "big")
    out.append(str(round(x, 1)))
    out.append(str(round(x, n % 5)))
    out.append(str(round(n, -(n % 4))))
    out.append(str(round(n * 0.5)))
    out.append(str(round(x, -1)))
    return ";".join(out)
