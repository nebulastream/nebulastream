# codon-fuzz: entry=udf args=VARSIZED,VARSIZED returns=VARSIZED
def udf(s, t):
    a = set(s)
    b = set(t)
    u = sorted(a | b)
    i = sorted(a & b)
    d = sorted(a - b)
    x = sorted(a ^ b)
    a.discard("a")
    try:
        b.remove("z")
        rm = "ok"
    except KeyError:
        rm = "missing"
    sub = set(s[:2]) <= a
    fs = frozenset(s) == frozenset(t)
    return "".join(u) + "|" + "".join(i) + "|" + "".join(d) + "|" + "".join(x) + "|" + rm + str(sub) + str(fs) + str(len(a))
