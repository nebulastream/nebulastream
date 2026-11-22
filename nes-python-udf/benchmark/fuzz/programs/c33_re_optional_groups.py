# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def udf(s):
    out = []
    for m in re.finditer(r"(\d+)(\.\d+)?", s):
        frac = m.group(2)
        out.append(m.group(1) + ("" if frac is None else frac) + "@" + str(m.span(2)))
    r = re.sub(r"(a)|(b)", lambda m: "A" if m.group(1) else "B", s)
    return ";".join(out) + "|" + r
