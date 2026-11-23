# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"]]
from collections import namedtuple
Base = namedtuple("Base", "head tail")


class Res(Base):
    pass


def udf(s):
    h, _, t = s.partition(":")
    r = Res(h, t)
    a, b = r
    return a + b
