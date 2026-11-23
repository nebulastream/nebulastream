# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class Base:
    def __init__(self, s: str):
        self.s = s

    def private(self):
        return self.s in self._constants._private


class V4(Base):
    pass


class V6(Base):
    pass


class _BaseConstants:
    _private = ["none"]


Base._constants = _BaseConstants


class _V4Constants:
    _private = ["a:b", "10"]


class _V6Constants:
    _private = ["zz"]


V4._constants = _V4Constants
V6._constants = _V6Constants


def udf(s):
    return str(Base(s).private()) + str(V4(s).private()) + str(V6(s).private())
