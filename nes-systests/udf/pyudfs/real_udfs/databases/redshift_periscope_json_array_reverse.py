# codon-fuzz: entry=json_array_reverse args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[\"home\", \"search\", \"cart\", \"checkout\"]"], ["[1, 2, 3]"], ["[]"], ["[{\"a\": 1}, [2, 3], \"x\", null]"], ["[\"solo\"]"]]
# real-udf: origin=databases id=redshift_periscope_json_array_reverse requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_json_arrays.rb#L119
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def json_array_reverse(j):
    import json
    if not j:
      return None
    try:
      arr = json.loads(j)
    except ValueError:
      return None
    return json.dumps(arr[::-1])
