# codon-fuzz: entry=udf_sum args=INT64,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[5, "mfa+"], [5, "mfa-"], [120, "mfa+"], [-3, "password"], [0, "mfa+"], [42, "MFA+"]]
# real-udf: origin=streaming id=risingwave_udf_sum requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with the RisingWave UDF server and does not change its logic.

# Source: https://github.com/risingwavelabs/risingwave/blob/6c258d07f7e4ca04f9e74a8198658840ff7218c7/integration_tests/feature-store/server/udf.py#L10-L15
# Repository: https://github.com/risingwavelabs/risingwave (commit 6c258d07f7e4ca04f9e74a8198658840ff7218c7)
# License: Apache-2.0
# System: RisingWave (arrow-udf)
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def udf_sum(x: int, y: str) -> int:
    if y == "mfa+":
        return x
    else:
        return -x
