# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re


def repl(m):
    w = m.group(0)
    return w[::-1].upper() if len(w) > 3 else "<" + w + ">"


def udf(s):
    a = re.sub(r"\w+", repl, s)
    b = re.sub(r"(?<=\d)(?=(\d{3})+$)", ",", "".join(c for c in s if c.isdigit()))
    c = re.subn(r"\s+", "_", s)
    return a + "|" + b + "|" + c[0] + str(c[1])
