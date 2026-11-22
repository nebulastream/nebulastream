# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["k=v"]]
import re


def udf(s):
    return ",".join(re.findall(r"(\w)=", s))
