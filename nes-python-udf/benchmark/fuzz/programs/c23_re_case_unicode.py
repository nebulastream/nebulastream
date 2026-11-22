# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.findall(r"(?i)ss", s)
    b = re.findall(r"(?i)[a-z]+", s)
    c = re.findall(r"(?i)é|ω|ǆ|k|s", s)
    d = re.sub(r"(?i)strasse", "X", s)
    e = bool(re.search(r"(?i)^hello", s))
    return ",".join(a) + "|" + ",".join(b) + "|" + ",".join(c) + "|" + d + "|" + str(e)
