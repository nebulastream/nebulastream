# codon-fuzz: entry=udf args=FLOAT64,INT64 returns=INT64
def udf(x, n):
    buckets = {}
    for k in range(5):
        key = round(x * k, 2) if x == x else 0.0
        buckets[key] = buckets.get(key, 0) + 1
    return len(buckets) * 10 + buckets.get(0.0, 0)
