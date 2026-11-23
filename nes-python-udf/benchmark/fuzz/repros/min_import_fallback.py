# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
try:
    from _speedups import fast_upper
except ImportError:
    def fast_upper(s):
        return s.upper()


def udf(s):
    return fast_upper(s)
