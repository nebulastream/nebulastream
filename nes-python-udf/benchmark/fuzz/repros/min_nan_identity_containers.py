# codon-fuzz: entry=udf args=FLOAT64 returns=BOOLEAN
# codon-fuzz-rows: 5
def udf(x):
    return x in [x]
