# codon-fuzz: entry=json_array_nth args=VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[\"home\", \"search\", \"product\", \"cart\", \"checkout\"]", 2], ["[\"home\", \"search\", \"product\", \"cart\", \"checkout\"]", 0], ["[10, 20, 30]", 2], ["[{\"lat\": 52.52, \"lng\": 13.405}, null]", 0], ["[\"a\", true, 1.5]", 1]]
# real-udf: origin=databases id=redshift_periscope_json_array_nth requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_json_arrays.rb#L59
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def json_array_nth(j, i):
    import json
    if not j or (not i and i != 0) or i < 0:
      return None
    try:
      arr = json.loads(j)
    except ValueError:
      return None
    if len(arr) <= i:
      return None
    return str(arr[i])
