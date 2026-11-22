# codon-fuzz: entry=udf args=VARSIZED,FLOAT64 returns=INT64
def udf(s, x):
    seen = set()
    for c in s[:10]:
        seen.add((c, x))
    seen.add(("", 0.0))
    seen.add(("", -0.0))
    return len(seen)
