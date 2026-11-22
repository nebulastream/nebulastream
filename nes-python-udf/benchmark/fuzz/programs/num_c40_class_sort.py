# codon-fuzz: entry=udf args=VARSIZED,INT64 returns=VARSIZED
class Rec:
    def __init__(self, k, v):
        self.k = k
        self.v = v
    def __lt__(self, other):
        return self.k < other.k
    def __gt__(self, other):
        return self.k > other.k
    def __repr__(self):
        return f"{self.k}{self.v}"

def udf(s, n):
    recs = [Rec(ord(c) % 4, i) for i, c in enumerate(s[:20])]
    a = sorted(recs)
    b = sorted(recs, reverse=True)
    c = max(recs) if recs else Rec(-1, -1)
    return repr(a) + "|" + repr(b) + "|" + repr(c)
