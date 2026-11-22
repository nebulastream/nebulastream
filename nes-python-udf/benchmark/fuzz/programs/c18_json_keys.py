# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import json


def udf(s):
    d = {}
    for i, w in enumerate(s.split()):
        d[w] = i
    d["zz"] = -1
    d["aa"] = len(s)
    a = json.dumps(d)
    b = json.dumps(dict(sorted(d.items())))
    c = json.dumps(list(d.keys()))
    e = json.dumps(list(d.values()))
    return a + "|" + b + "|" + c + "|" + e
