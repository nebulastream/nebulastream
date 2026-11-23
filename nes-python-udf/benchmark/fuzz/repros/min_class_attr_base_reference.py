# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class Base:
    def __init__(self, s: str):
        self.s = s

    def tag(self):
        return self.prefix + self.s


class Sub(Base):
    pass


Base.prefix = "b:"
Sub.prefix = "s:"


def udf(s):
    items = [Base(s), Sub(s)]
    return "|".join(x.tag() for x in items)
