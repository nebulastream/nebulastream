# codon-fuzz: entry=udf args=INT64 returns=INT64
# codon-fuzz-rows: 5
# Before Codon fork 9dd43c0, a **mapping item in a dict display aborted the compiler.
def udf(n):
    d = {"a": n}
    return len({**d, "x": 0})
