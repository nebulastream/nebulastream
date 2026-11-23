# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
def _split(s):
    h, _, t = s.partition(":")
    return h, t


class Pair:
    def __init__(self, s: str):
        self.head, self.tail = _split(s)
        (self.n, self.m) = len(self.head), len(self.tail)


def udf(s):
    p = Pair(s)
    return p.tail + p.head + str(p.n + p.m)
