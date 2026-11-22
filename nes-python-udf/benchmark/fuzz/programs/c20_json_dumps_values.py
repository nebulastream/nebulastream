# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
import json


def udf(s, n):
    a = json.dumps([str(n), s[:3]]) + json.dumps([True, False]) + json.dumps([n, -n])
    b = json.dumps((1, 2))
    c = json.dumps({1: "int key", 2: "two"})
    d = json.dumps({"t": True})
    e = json.dumps([[1], [], [2, 3]])
    return "|".join([a, b, c, d, e])
