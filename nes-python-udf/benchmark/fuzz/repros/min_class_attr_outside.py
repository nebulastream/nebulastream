# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class Counter:
    def __init__(self, s: str):
        self.s = s

    def tag(self):
        return self.prefix + self.s


Counter.prefix = "n="


def udf(s):
    return Counter(s).tag() + Counter.prefix
