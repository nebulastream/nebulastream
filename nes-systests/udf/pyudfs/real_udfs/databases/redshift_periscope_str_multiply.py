# codon-fuzz: entry=str_multiply args=VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["ab", 3], ["-", 20], ["*", 0], ["Ha", 5], ["x", -2], ["€", 4]]
# real-udf: origin=databases id=redshift_periscope_str_multiply requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L162
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def str_multiply(str, times):
    if not str:
      return None
    return str * times
