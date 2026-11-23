# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class Base:
    prefix = "b:"

    def __init__(self, s: str):
        self.s = s

    def tag(self):
        return self.prefix + self.s


class Sub(Base):
    prefix = "s:"


def udf(s):
    return Base(s).tag() + Sub(s).tag()
