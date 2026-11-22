# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["a b"]]
import re


def udf(s):
    return ",".join(re.findall(r"a # comment", s, re.VERBOSE))
