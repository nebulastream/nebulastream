# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
import json


def udf(s, n):
    x = n / 7
    if x != x or x in (float("inf"), float("-inf")):
        x = 0.0
    doc = {"name": s, "nested": s[:2], "up": s.upper()}
    a = json.dumps(doc)
    b = json.dumps([s, s[::-1]])
    back = json.loads(a)
    return a + "|" + b + "|" + str(back["name"]) + json.dumps({"n": n, "m": -n})
