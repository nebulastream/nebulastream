# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["12"]]
class Point:
    def __init__(self, x: str, y: str):
        self.x = x
        self.y = y

    @classmethod
    def parse(cls, s):
        a, _, b = s.partition(":")
        return cls(a, b)


def udf(s):
    p = Point.parse(s)
    return p.x + "|" + p.y
