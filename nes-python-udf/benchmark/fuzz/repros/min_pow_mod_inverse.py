# codon-fuzz: entry=udf args=INT64 returns=INT64
# codon-fuzz-rows: 5
def udf(n):
    return pow(3, -1, 7)
