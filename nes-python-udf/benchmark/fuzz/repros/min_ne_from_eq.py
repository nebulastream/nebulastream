# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
from functools import total_ordering


@total_ordering
class V:
    def __init__(self, s: str):
        self.s = s

    def __eq__(self, other) -> bool:
        return self.s == other.s

    def __lt__(self, other) -> bool:
        return len(self.s) < len(other.s)


def udf(s):
    return str(V(s) >= V("ab")) + str(V(s) > V("x")) + str(V(s) <= V("x"))
