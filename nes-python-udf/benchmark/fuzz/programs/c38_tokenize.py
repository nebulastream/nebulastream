# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
import re

TOKEN = re.compile(r"\s*(?:(\d+\.?\d*)|(\w+)|(.))")


def udf(s):
    toks = []
    pos = 0
    while pos < len(s):
        m = TOKEN.match(s, pos)
        if m is None or m.end() == pos:
            break
        num, word, op = m.group(1), m.group(2), m.group(3)
        if num is not None:
            toks.append("N:" + num)
        elif word is not None:
            toks.append("W:" + word)
        elif op is not None:
            toks.append("O:" + op)
        pos = m.end()
    return " ".join(toks)
