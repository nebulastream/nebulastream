# codon-fuzz: entry=json_array_concat args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[1, 2, 3]", "[4, 5]"], ["[\"red\", \"green\"]", "[\"blue\"]"], ["", "[\"only\", \"right\"]"], ["[\"left\"]", ""], ["[{\"sku\": \"A1\", \"qty\": 2}]", "[{\"sku\": \"B7\", \"qty\": 1}]"], ["[]", "[]"]]
# real-udf: origin=databases id=redshift_periscope_json_array_concat requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_json_arrays.rb#L198
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def json_array_concat(j, k):
    import json
    if not j:
      return k
    if not k:
      return j
    try:
      arr_j = json.loads(j)
      arr_k = json.loads(k)
    except ValueError:
      return None
    arr_j.extend(arr_k)
    return json.dumps(arr_j)
