# codon-fuzz: entry=udf args=FLOAT64 returns=BOOLEAN
# codon-fuzz-rows: 5
def udf(x):
    t = (x,)
    return t == t
