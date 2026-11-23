# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["x"]]
class V:
    def __init__(self, s: str):
        self.s = s

    def __lt__(self, other: "V") -> bool:
        return len(self.s) < len(other.s)


def udf(s):
    return str(V(s) < V("ab"))
