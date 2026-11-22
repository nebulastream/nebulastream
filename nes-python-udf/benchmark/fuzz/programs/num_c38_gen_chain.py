# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def chunks(xs, k):
    for i in range(0, len(xs), k):
        yield xs[i:i + k]

def running(xs):
    total = 0
    for v in xs:
        total += v
        yield total

def gen_with_return(n):
    for i in range(n):
        if i == 3:
            return
        yield i

def udf(s, n):
    k = n % 4 + 1
    parts = list(chunks(s, k))
    sums = list(running(len(p) for p in parts))
    g = gen_with_return(n % 6)
    first = next(g, -1)
    rest = list(g)
    exhausted = next(g, -9)
    return "/".join(parts) + str(sums) + str(first) + str(rest) + str(exhausted)
