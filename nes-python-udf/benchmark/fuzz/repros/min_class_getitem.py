# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
import types


class Pair:
    __class_getitem__ = classmethod(types.GenericAlias)

    def __init__(self, a: str, b: str):
        self.a = a
        self.b = b


def udf(s):
    p = Pair(s, s.upper())
    return p.a + p.b
