# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
import json


def udf(x):
    if x != x or abs(x) == float("inf"):
        return "skip"
    return json.dumps([x]) + "|" + str(x)
