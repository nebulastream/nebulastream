# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[5]]


class Base:
    def f(self, a):
        return "b" + str(a)


class Sub(Base):
    def f(self, a):
        return "s" + str(a)


def call(g, a):
    return g(a)


def udf(n):
    return call(Sub().f, n) + call(Base().f, n)
