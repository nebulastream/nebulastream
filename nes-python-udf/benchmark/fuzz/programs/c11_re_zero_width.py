# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.sub(r"(?=[A-Z])", "_", s)
    b = re.sub(r"\b", "|", s)
    c = re.split(r"(?<=[.!?])\s*", s)
    d = re.findall(r"(?<![a-z])\d+(?![a-z])", s)
    e = re.sub(r"x*", "-", s[:10])
    return a + "#" + b + "#" + "/".join(str(p) for p in c) + "#" + ",".join(d) + "#" + e
