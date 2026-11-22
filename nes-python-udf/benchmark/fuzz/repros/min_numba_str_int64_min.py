# codon-fuzz: entry=udf args=INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[-9223372036854775808]]
# Numba (--compiler Numba): str() of INT64_MIN throws std::bad_alloc out of the UDF and ends the process.
def udf(n):
    return str(n)
