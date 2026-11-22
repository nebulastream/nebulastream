# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["{\"a\": 1, \"a\": 2}"]]
import json


def udf(s):
    return json.dumps(json.loads(s))
