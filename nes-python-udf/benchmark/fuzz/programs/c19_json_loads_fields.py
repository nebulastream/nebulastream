# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["{\"a\": 1.50, \"b\": \"x\\ty\", \"c\": [1, 2], \"d\": {\"e\": true}}"], ["{\"a\": 1e2}"], ["{\"a\": -0}"], ["{\"b\": \"\\u00e9\\ud83d\\ude00\"}"], ["{\"a\": 12345678901234567890}"], ["{\"a\": NaN}"], ["{\"a\": 1, \"a\": 2}"], [" {\"a\" : 0.1 } "]]
import json


def udf(s):
    try:
        doc = json.loads(s)
    except Exception:
        return "invalid"
    out = []
    for k in doc:
        out.append(str(k) + "=" + json.dumps(doc[k]))
    return ";".join(out)
