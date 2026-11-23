# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"]]
from collections import namedtuple
Base = namedtuple("Base", "head tail")


def udf(s):
    h, _, t = s.partition(":")
    return str(Base(h, t)) + repr(Base(h, t))
