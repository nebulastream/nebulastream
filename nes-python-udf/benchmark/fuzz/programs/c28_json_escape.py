# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["\"a\\\\b\\/c\\b\\f\\n\\r\\t\""], ["\"\\u0000\\u001f\\u007f\""], ["\"\\uD83D\\uDE00\""], ["\"\\ud800\""], ["\"tab\there\""]]
import json


def udf(s):
    try:
        v = json.loads(s)
    except Exception:
        return "bad"
    t = str(v)
    return json.dumps(t) + "|" + str(len(t)) + "|" + json.dumps([t])
