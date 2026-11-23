# codon-fuzz: entry=udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["a:b"], ["zz"]]
try:
    from functools import cache
except (ImportError, AttributeError):
    cache = None
HAVE = cache is not None


def udf(s):
    return s + str(HAVE)
