# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import json


def udf(s):
    a = json.dumps(s)
    b = json.dumps([s, s.upper()])
    c = json.dumps({s: [s, s[:1]]})
    d = json.loads(a)
    return a + "|" + b + "|" + c + "|" + str(d == s)
