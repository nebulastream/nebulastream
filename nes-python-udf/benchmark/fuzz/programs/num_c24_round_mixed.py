# codon-fuzz: entry=udf args=INT64,FLOAT64 returns=FLOAT64
def udf(n, x):
    total = 0.0
    for k in range(1, 4):
        total += round(n / k, 1)
    return total + round(n, -1) * 0.0 + round(x, n % 3)
