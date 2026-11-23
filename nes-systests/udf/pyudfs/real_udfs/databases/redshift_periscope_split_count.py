# codon-fuzz: entry=split_count args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["red,green,blue", ","], ["a||b||c||", "||"], ["no delimiter here", ";"], ["/usr/local/bin/python3", "/"], ["tag1 tag2  tag3", " "]]
# real-udf: origin=databases id=redshift_periscope_split_count requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L128
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def split_count(str, delim):
    if not str or not delim:
      return None
    return len(str.split(delim))
