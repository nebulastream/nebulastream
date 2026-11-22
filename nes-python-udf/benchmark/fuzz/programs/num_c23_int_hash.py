# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
def udf(n):
    s = {n % 10, (n // 10) % 10, 1, int(True)}
    d = {1: "a"}
    d[int(True)] = "b"
    d[int(1.0 == 1)] = "c"
    return str(sorted(s)) + str(len(d)) + d[1] + str(hash(n) == hash(n)) + str(hash(-1) == hash(-2))
