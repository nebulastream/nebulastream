# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class Pair:
    def __init__(self, s: str):
        self.head = self._split(s)[0]

    def _split(self, s):
        h, _, t = s.partition(":")
        return h, t


def udf(s):
    return Pair(s).head
