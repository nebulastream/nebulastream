# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["a"]]
import re


def udf(s):
    return ";".join(x + "/" + y for x, y in re.findall(r"(a)(b)?", s))
