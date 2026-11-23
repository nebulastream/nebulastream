# codon-fuzz: entry=udf args=FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.0], [-0.0], [0.1], [1.0], [126.5], [1e16], [1e15], [0.0001], [1e-05], [1.5e-300], [5e-324], [1.7976931348623157e308], [123456789.123], [-2500.0], [3.0e22]]
# str, repr and f-strings without a spec write a float as CPython's repr does (shortest round-trip digits). Codon wrote %g.


def udf(x):
    return str(x) + " " + repr(x / 3) + " " + f"{x * 7}"
