# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.sub(r"\d*", "#", s[:12])
    b = re.findall(r"\w*", s[:12])
    c = re.sub(r"a|", "-", s[:8])
    d = re.sub(r"\s*", "", s)
    return a + "|" + ",".join(b) + "|" + c + "|" + d
