# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 5
# Before Codon fork ae5373a, the second loop over the exhausted generator crashed (SIGSEGV).
def gen(n):
    for i in range(n):
        yield i

def udf(n):
    g = gen(n % 3)
    return str(list(g)) + str(list(g))
