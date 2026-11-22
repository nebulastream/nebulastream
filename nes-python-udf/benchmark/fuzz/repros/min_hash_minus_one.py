# codon-fuzz: entry=udf args=INT64 returns=BOOLEAN
# codon-fuzz-rows: 5
def udf(n):
    return hash(-1) == hash(-2)
