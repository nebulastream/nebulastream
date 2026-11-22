# codon-fuzz: entry=udf args=INT64,FLOAT64 returns=VARSIZED
def udf(n, x):
    r = []
    r.append(n == x)
    r.append(n < x)
    r.append(float(n) == n)
    r.append(int(float(n)) == n if abs(n) < 2**62 else False)
    r.append(n + 0.0 == n)
    r.append(max(n, x) == x)
    r.append(int(True) + n == n + 1)
    r.append(n % 3 == 0 or x > 0)
    return ",".join(str(v) for v in r)
