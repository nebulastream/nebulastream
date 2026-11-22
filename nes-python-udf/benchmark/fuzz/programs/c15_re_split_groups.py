# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.split(r"([,;])", s)
    b = re.split(r"\s+", s, maxsplit=2)
    c = re.split(r"(-)|(_)", s)
    d = re.split(r"", s[:6])
    return "[" + "][".join(str(x) for x in a) + "]#[" + "][".join(str(x) for x in b) + "]#[" + "][".join(str(x) for x in c) + "]#[" + "][".join(str(x) for x in d) + "]"
