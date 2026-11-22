# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["1e400"], ["-Infinity"], ["NaN"]]
import json


def udf(s):
    return str(json.loads(s) == json.loads(s))
