# codon-fuzz: entry=udf args=VARSIZED,VARSIZED returns=VARSIZED


def udf(s, t):
    words = (s + " " + t).split()
    a = sorted(words, key=str.lower)
    b = sorted(words, key=len, reverse=True)
    c = sorted(words, key=lambda w: w.casefold(), reverse=True)
    d = max(words, key=len) if words else ""
    e = min(words) if words else ""
    return " ".join(a) + "|" + " ".join(b) + "|" + " ".join(c) + "|" + d + "|" + e + "|" + str(s < t) + str(s.casefold() == t.casefold())
