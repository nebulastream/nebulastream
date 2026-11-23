# codon-fuzz: entry=str_index args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["jane.doe@example.com", "@"], ["https://example.com/a/b", "/"], ["Mississippi", "ss"], ["hello", "z"], ["2024-03-15T14:22:05Z", "T"]]
# real-udf: origin=databases id=redshift_periscope_str_index requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L180
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def str_index(full_str, find_substr):
    if not full_str or not find_substr:
      return None
    return full_str.find(find_substr)
