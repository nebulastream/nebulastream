# codon-fuzz: entry=format_num args=FLOAT64,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1234567.891, ",.2f"], [0.4567, ".1%"], [42.0, "d"], [255.0, "x"], [3.14159, ".3e"], [98765.4321, "_.1f"], [7.5, ">10.2f"]]
# real-udf: origin=databases id=redshift_periscope_format_num requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_number_utils.rb#L5
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def format_num(num, format):
    if not num or not format:
      return None
    try:
      return ("{:" + format + "}").format(num)
    except ValueError:
      try:
        return ("{:" + format + "}").format(int(num))
      except ValueError:
        return None
