# codon-fuzz: entry=udf args=VARSIZED returns=INT64
# codon-fuzz-inputs: [["\"\\ud800\""]]
import json


def udf(s):
    return len(str(json.loads(s)))
