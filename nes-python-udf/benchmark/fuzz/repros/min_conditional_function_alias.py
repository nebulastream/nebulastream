# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"]]
def conv(s):
    if isinstance(s, str):
        def f(x: str) -> str:
            return x.upper()
        g = f
    else:
        def h(x: str) -> str:
            return x
        g = h
    return g(s)


def udf(s):
    return conv(s)
