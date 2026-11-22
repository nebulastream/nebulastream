# codon-fuzz: entry=udf args=INT64 returns=FLOAT64
# codon-fuzz-rows: 5
def udf(n):
    p = 10 ** -(n % 3 + 1)
    return p * 1.0
