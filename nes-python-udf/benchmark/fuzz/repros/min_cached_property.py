# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
import functools
from functools import cached_property


class Net:
    def __init__(self, s: str):
        self.s = s

    @cached_property
    def upper(self):
        return self.s.upper()

    @functools.cached_property
    def size(self):
        return len(self.upper) * 2


def udf(s):
    n = Net(s)
    return n.upper + str(n.size) + n.upper
