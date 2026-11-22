# codon-fuzz: entry=udf args=FLOAT64 returns=INT64
# codon-fuzz-rows: 5
def udf(x):
    return len({x})
