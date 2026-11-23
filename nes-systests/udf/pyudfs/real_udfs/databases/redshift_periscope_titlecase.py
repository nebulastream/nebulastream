# codon-fuzz: entry=titlecase args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["the quick brown fox"], ["NEW YORK CITY"], ["o'neil's pub"], ["mcdonald-smith"], ["über straße 12b"], ["hello   world"]]
# real-udf: origin=databases id=redshift_periscope_titlecase requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L145
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def titlecase(str):
    if not str:
      return None
    return str.title()
