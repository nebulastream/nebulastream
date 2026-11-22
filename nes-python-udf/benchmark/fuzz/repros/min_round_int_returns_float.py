# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 5
def udf(n):
    return type(round(n)).__name__ + "|" + str(round(n, -2))
