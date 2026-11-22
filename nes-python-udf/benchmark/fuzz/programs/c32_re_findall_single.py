# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.findall(r"(\d+)", s)
    b = re.findall(r"(?:\w)+", s)
    c = re.findall(r"#(\w+)", "#tag " + s)
    d = [str(m.group(1)) for m in re.finditer(r"(\w)\w*", s)]
    return ",".join(a) + "|" + ",".join(b) + "|" + ",".join(c) + "|" + ",".join(d)
