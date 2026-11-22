# codon-fuzz: entry=udf args=VARSIZED,FLOAT64 returns=VARSIZED
import json


def udf(s, x):
    if x != x or abs(x) == float("inf"):
        x = 1.5
    rec = {"metrics": {"x": x, "half": x / 2, "sq": x * x}, "hist": {"a": x, "r": round(x, 2), "e": 1e-5}}
    return json.dumps(rec)
