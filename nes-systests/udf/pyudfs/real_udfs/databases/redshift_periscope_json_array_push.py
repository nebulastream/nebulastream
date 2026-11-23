# codon-fuzz: entry=json_array_push args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[\"home\", \"search\"]", "cart"], ["", "first"], ["not json", "recovered"], ["[1, 2, 3]", "4"], ["[\"Zürich\"]", "Genève"]]
# real-udf: origin=databases id=redshift_periscope_json_array_push requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_json_arrays.rb#L171
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def json_array_push(j, value):
    import json
    if not j:
      arr = []
    else:
      try:
        arr = json.loads(j)
      except ValueError:
        arr = []
    arr.append(value)
    return json.dumps(arr)
