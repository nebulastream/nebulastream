# codon-fuzz: entry=json_array_pop args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[\"home\", \"search\", \"cart\"]"], ["[1, 2, 3, 4]"], ["[]"], ["[{\"step\": 1}, {\"step\": 2}]"], ["[\"only\"]"]]
# real-udf: origin=databases id=redshift_periscope_json_array_pop requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_json_arrays.rb#L144
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def json_array_pop(j):
    import json
    if not j:
      return None
    try:
      arr = json.loads(j)
    except ValueError:
      return None
    if len(arr) > 0:
      arr.pop()
    return json.dumps(arr)
