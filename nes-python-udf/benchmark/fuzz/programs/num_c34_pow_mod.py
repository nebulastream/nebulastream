# codon-fuzz: entry=udf args=INT64 returns=INT64
def udf(n):
    m = n % 11 + 2
    inv = pow(n % m or 1, -1, m) if (n % m) % 2 else 0
    return inv
