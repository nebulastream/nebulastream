# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["{\"b\": 1, \"a\": [1, 2.50, -0.0, 1e400], \"c\": \"\\u00e9\\ud83d\\ude00\"}"], ["[1, 2, 3]"], ["{\"x\": {\"y\": null}}"], ["\"str\""], ["12.0"]]
import json


def udf(s):
    try:
        v = json.loads(s)
    except Exception:
        return "invalid"
    return json.dumps(v)
