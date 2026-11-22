# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["a"]]
import re


def udf(s):
    return re.sub(r"$", "<", s)
