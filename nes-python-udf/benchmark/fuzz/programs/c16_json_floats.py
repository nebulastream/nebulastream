# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
import json


def udf(n):
    vals = [n / 7, n * 0.1, float(n), n / 1e10, 1e16 * (n % 5), 0.1 + 0.2, -0.0, 1e22, 1e-7]
    a = json.dumps(vals)
    b = json.dumps({"v": n / 3, "i": n})
    return a + "|" + b
