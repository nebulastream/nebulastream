# codon-fuzz: entry=udf args=INT64 returns=INT64
# codon-fuzz-rows: 5
def udf(n):
    fs = [lambda: k for k in range(3)]
    return fs[0]()
