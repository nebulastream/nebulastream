# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    a = re.sub(r"(\w)\1+", r"\1", s)
    b = re.sub(r"(?P<w>\w+) (?P=w)", r"\g<w>", s)
    c = re.findall(r"(a)(b)?", s)
    d = re.sub(r"(\w+)@(\w+)", r"\2 at \1", s)
    m = re.fullmatch(r"(\w)(\w*)", s)
    e = (m.group(2) + m.group(1) + str(m.span(2))) if m else "nomatch"
    return a + "#" + b + "#" + ";".join(x + "/" + y for x, y in c) + "#" + d + "#" + e
