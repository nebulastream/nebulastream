# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["1.1"], ["2"]]
import json


def udf(s):
    return str(float(json.loads(s)) * 3)
