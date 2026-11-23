# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["x"]]
def conv(s, upper):
    if isinstance(s, str):
        def f(x):
            return x.upper() if upper else x
    else:
        def f(x):
            return str(x)
    return f(s)


def udf(s):
    return conv(s, True) + conv(s, False)
