# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def depth(s, i=0):
    if i >= len(s):
        return 0
    if s[i] == "(":
        return 1 + depth(s, i + 1)
    if s[i] == ")":
        return -1 + depth(s, i + 1)
    return depth(s, i + 1)

def ack(m, n):
    if m == 0:
        return n + 1
    if n == 0:
        return ack(m - 1, 1)
    return ack(m - 1, ack(m, n - 1))

def perms(xs):
    if len(xs) <= 1:
        return [xs]
    out = []
    for i in range(len(xs)):
        for p in perms(xs[:i] + xs[i + 1:]):
            out.append(xs[i:i + 1] + p)
    return out

def udf(s, n):
    return f"{depth(s)}|{ack(2, n % 5)}|{len(perms(s[:4]))}|{sorted(perms(s[:3]))[:2]}"
