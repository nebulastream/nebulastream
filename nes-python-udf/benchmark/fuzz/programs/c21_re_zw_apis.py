# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = [m.span() for m in re.finditer(r"(?=\d)", s)]
    b = re.findall(r"(?<=a)", s)
    c = re.split(r"(?=[A-Z])", s)
    d = re.subn(r"\B", ".", s[:8])
    e = re.sub(r"^", ">", s)
    f = re.sub(r"$", "<", s)
    return str(a) + "#" + str(len(b)) + "#" + "/".join(str(x) for x in c) + "#" + d[0] + str(d[1]) + "#" + e + "#" + f
