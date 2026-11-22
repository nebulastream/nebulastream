# codon-fuzz: entry=udf args=INT64,FLOAT64 returns=FLOAT64
def udf(n, x):
    e = n % 6 - 3
    scale = 10 ** e
    return x * scale
