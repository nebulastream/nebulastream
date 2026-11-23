# codon-fuzz: entry=add_one args=INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0], [41], [-1], [1700000000000], [9007199254740993], [-273]]
# real-udf: origin=streaming id=pyflink_playgrounds_add_one requires= wrapped=1 ported=0
# Wrapper: removed the @udf(...) decorator; it only registers the function with PyFlink and does not change its logic.

# Source: https://github.com/pyflink/playgrounds/blob/cacfa58a8ae4bfe5880f08ca3bdc6b2a75585f72/examples/table/utils/udfs.py#L5-L7
# Repository: https://github.com/pyflink/playgrounds (commit cacfa58a8ae4bfe5880f08ca3bdc6b2a75585f72)
# License: Apache-2.0
# System: PyFlink
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

def add_one(i):
    return i + 1
