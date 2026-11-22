# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    b = re.sub(r"(?<![\w])(?=\w)", "^", s[:20])
    c = re.findall(r"(?=(\w\w))", s[:10])
    return a + "|" + b + "|" + ",".join(c)
