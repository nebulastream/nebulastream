# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["1000"], ["1234567"]]
import re


def udf(s):
    return re.sub(r"(?=\d)", ",", s)
