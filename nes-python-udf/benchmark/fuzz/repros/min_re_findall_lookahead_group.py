# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["abc"]]
import re


def udf(s):
    return ",".join(re.findall(r"(?=(\w\w))", s))
