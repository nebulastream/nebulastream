# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
def udf(s, n):
    d = {}
    for i, c in enumerate(s):
        d[c] = i
        if i % 3 == 2 and s[i - 1] in d:
            del d[s[i - 1]]
    d.setdefault("z", n % 10)
    d.update({"a": 1})
    d["b"] = 2
    p = d.pop("q", -1)
    d2 = dict(d)
    for k in list(d2):
        if d2[k] % 2:
            d2.pop(k)
    d2["late"] = 0
    keys = list(d2.keys())
    return str(keys[:8]) + "|" + str(p) + "|" + str(sorted(d.items())[:5]) + "|" + str(len(d))
