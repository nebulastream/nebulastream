# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"], ["x"]]
def conv(s):
    if isinstance(s, str):
        def count(x: str, n: int) -> int:
            return n if not x else count(x[1:], n + 1)
    else:
        def count(x: str, n: int) -> int:
            return 0
    return str(count(s, 0))


def udf(s):
    return conv(s)
