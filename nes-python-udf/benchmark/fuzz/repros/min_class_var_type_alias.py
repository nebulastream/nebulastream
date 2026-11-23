# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
class K:
    X = "k"


class C:
    K = K

    def x(self):
        return self.K.X


def udf(s):
    return C().x() + s
