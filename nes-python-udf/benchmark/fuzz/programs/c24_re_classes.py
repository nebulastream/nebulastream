# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.findall(r"[\w]", s)
    b = re.findall(r"\W+", s)
    c = re.findall(r"\b\w", s)
    d = re.findall(r"[^\s\d]+", s)
    e = re.findall(r"[\u0370-\u03ff]+", s)
    f = re.findall(r"[^\x00-\x7f]", s)
    return "".join(a) + "|" + ",".join(b) + "|" + "".join(c) + "|" + ",".join(d) + "|" + ",".join(e) + "|" + "".join(f)
