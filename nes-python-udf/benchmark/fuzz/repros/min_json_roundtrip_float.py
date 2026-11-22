# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["[2.0]"], ["{\"a\": 2.0}"]]
import json


def udf(s):
    return json.dumps(json.loads(s))
