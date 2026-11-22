# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["İı"]]
import re


def udf(s):
    return ",".join(re.findall(r"(?i)i", s))
