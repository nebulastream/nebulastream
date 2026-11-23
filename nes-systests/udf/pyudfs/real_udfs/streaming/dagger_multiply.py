# codon-fuzz: entry=multiply args=FLOAT64,FLOAT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[12.5, 3.0], [19.99, 4.0], [-2.5, 8.25], [1000000.0, 0.0175], [0.1, 0.2], [7.0, 6.0]]
# real-udf: origin=streaming id=dagger_multiply requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/raystack/dagger/blob/9aac71e9c2d1b4f0cb1d5dc5a561a312edea37cf/dagger-py-functions/udfs/scalar/multiply.py#L5-L7
# Repository: https://github.com/raystack/dagger (commit 9aac71e9c2d1b4f0cb1d5dc5a561a312edea37cf)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def multiply(i, j):
    return i * j
