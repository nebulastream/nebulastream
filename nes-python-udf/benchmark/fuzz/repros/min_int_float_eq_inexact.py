# codon-fuzz: entry=udf args=INT64 returns=BOOLEAN
# codon-fuzz-rows: 40
def udf(n):
    return float(n) == n
