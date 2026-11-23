# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["12"]]
class Counter:
    PREFIX = "n="

    def __init__(self, n: int):
        self.n = n

    @classmethod
    def label(cls, n):
        return cls.PREFIX + str(n)

    def show(self):
        return self.label(self.n)


def udf(s):
    c = Counter(len(s))
    return c.show() + c.label(7)
