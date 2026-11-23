# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[-5]]
# Before Codon fork 641f329, with Python numerics (PYTHON_COMPAT), bin/hex/oct of a negative int never returned
# (_int_format looped on b //= base).
def udf(n):
    return bin(n)
