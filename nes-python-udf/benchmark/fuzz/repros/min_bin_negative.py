# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[-5]]
# With NES_CODON_PY_NUMERICS=1, bin/hex/oct of a negative int never return (_int_format loops on b //= base).
def udf(n):
    return bin(n)
