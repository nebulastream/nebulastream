# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["x"]]
def conv(s):
    if len(s) > 1:
        def f(x: str) -> str:
            return x.upper()
    else:
        def f(x: str) -> str:
            return x + x
    return f(s)


def udf(s):
    return conv(s)
