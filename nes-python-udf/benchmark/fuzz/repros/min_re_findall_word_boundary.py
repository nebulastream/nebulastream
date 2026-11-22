# codon-fuzz: entry=udf args=VARSIZED returns=INT64
# codon-fuzz-inputs: [["abc"]]
import re


def udf(s):
    return len(re.findall(r"\b", s))
