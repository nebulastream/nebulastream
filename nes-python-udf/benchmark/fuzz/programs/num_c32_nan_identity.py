# codon-fuzz: entry=udf args=FLOAT64 returns=BOOLEAN
def udf(x):
    t = (x, 1)
    return t == t
