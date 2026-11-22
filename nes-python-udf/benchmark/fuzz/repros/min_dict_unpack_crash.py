# codon-fuzz: entry=udf args=INT64 returns=INT64
# codon-fuzz-rows: 5
def udf(n):
    d = {"a": n}
    return len({**d, "x": 0})
