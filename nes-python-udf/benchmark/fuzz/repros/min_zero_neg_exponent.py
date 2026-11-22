# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 5
def udf(n):
    return str((n % 2) ** -1)
