# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-inputs: [["first line\nSecond Line\nthird"], ["a.b\nc"], ["ABC abc Abc"]]
import re


def udf(s):
    a = re.findall(r"^\w+", s, re.MULTILINE)
    b = re.findall(r"\w+$", s, re.M)
    c = re.findall(r"a.b", s, re.DOTALL)
    d = re.findall(r"abc", s, re.I)
    e = re.findall(r"""
        [a-z]+   # letters
        \s?      # optional space
    """, s, re.VERBOSE)
    f = re.findall(r"\w+", s, re.ASCII)
    return "|".join([",".join(a), ",".join(b), ",".join(c), ",".join(d), ",".join(e), ",".join(f)])
