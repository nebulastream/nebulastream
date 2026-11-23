# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[5]]


class Base:
    def check(self, a):
        return "b" + str(a)


class Sub(Base):
    def check(self, a):
        return "s" + str(a)


def udf(n):
    return Sub().check(n)
