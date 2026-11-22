# codon-fuzz: entry=udf args=VARSIZED,VARSIZED returns=VARSIZED


def udf(s, t):
    pairs = [(s, t), (s.lower(), t.lower()), (s[:1], t[:1]), (s + "\U0001f600", s + "\uffff")]
    out = []
    for a, b in pairs:
        out.append(("<" if a < b else ">" if a > b else "=") + str(a <= b))
    words = sorted({s, t, s.upper(), t.title(), "\uffff", "\U00010000"})
    return "".join(out) + "|" + ",".join(hex(ord(w[0])) if w else "e" for w in words)
