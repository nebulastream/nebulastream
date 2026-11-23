# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["12"]]
class Base:
    BITS = 0
    NAME = "base"

    @classmethod
    def all_ones(cls):
        return (1 << cls.BITS) - 1

    @classmethod
    def describe(cls, s):
        return cls.NAME + ":" + s + ":" + str(cls.all_ones())


class V4(Base):
    BITS = 32
    NAME = "v4"


class V6(Base):
    BITS = 48
    NAME = "v6"


def udf(s):
    return V4.describe(s) + " " + V6.describe(s) + " " + Base.describe(s)
