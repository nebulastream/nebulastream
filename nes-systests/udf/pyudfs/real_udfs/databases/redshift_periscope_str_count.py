# codon-fuzz: entry=str_count args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["banana", "an"], ["GET /index.html HTTP/1.1", "/"], ["aaaa", "aa"], ["Mississippi", "ss"], ["hello world", "xyz"]]
# real-udf: origin=databases id=redshift_periscope_str_count requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L217
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def str_count(full_str, find_substr):
    if not full_str or not find_substr:
      return None
    return full_str.count(find_substr)
