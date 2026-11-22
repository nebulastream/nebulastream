# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def udf(s, n):
    d = {}
    for i, c in enumerate(s):
        if c in d:
            del d[c]
        d[c] = i
    for k in list(d)[: n % 3]:
        d.pop(k)
    d["first"] = -1
    d.setdefault(s[:1], 42)
    e = dict(d)
    e["x"] = 0
    pi = e.popitem() if e else ("", 0)
    keys = "".join(k[:1] for k in e)
    vals = list(d.values())[:5]
    return keys + "|" + str(pi) + "|" + str(vals) + "|" + str(list(reversed(list(d)))[:3])
