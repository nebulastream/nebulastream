# codon-fuzz: entry=json_array_sort args=VARSIZED,BOOLEAN returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[5, 3, 9, 1, 7]", true], ["[5, 3, 9, 1, 7]", false], ["[\"pear\", \"apple\", \"fig\", \"Banana\"]", true], ["[\"2024-03-01\", \"2023-12-31\", \"2024-01-15\"]", false], ["[2.5, -1.0, 2.25]", true]]
# real-udf: origin=databases id=redshift_periscope_json_array_sort requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_json_arrays.rb#L89
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def json_array_sort(j, ascending):
    import json
    if not j:
      return None
    try:
      arr = json.loads(j)
    except ValueError:
      return None
    if not ascending:
      arr = sorted(arr, reverse=True)
    else:
      arr = sorted(arr)
    return json.dumps(arr)
