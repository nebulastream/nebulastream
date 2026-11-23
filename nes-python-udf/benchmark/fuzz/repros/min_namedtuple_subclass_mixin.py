# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["xy:z"]]
from collections import namedtuple


Base = namedtuple("Base", "head tail")


class Mixin(object):
    __slots__ = ()

    @property
    def size(self):
        return len(self.head) + len(self.tail)

    def joined(self, sep: str) -> str:
        return self.head + sep + self.tail


class Res(Base, Mixin):
    __slots__ = ()

    def geturl(self):
        return self.head + "/" + self.tail


def udf(s):
    h, _, t = s.partition(":")
    r = Res(h, t)
    a, b = r
    return r.geturl() + "|" + r.joined("-") + "|" + str(r.size) + "|" + a + b + "|" + str(len(r)) + "|" + r[0] + "|" + str(r == Res(h, t))
