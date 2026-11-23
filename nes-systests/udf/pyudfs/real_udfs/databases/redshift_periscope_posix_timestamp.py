# codon-fuzz: entry=case_entry args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2024-03-15 14:22:05"], ["2023-12-31 23:59:59"], ["2021-01-03 08:05:00"], ["2020-02-29 00:00:00.250000"], ["2019-07-04 17:45:30"], ["2024-12-30 06:10:42"], ["1969-07-20 20:17:40"]]
# real-udf: origin=databases id=redshift_periscope_posix_timestamp requires= wrapped=1 ported=0
# Wrapper: Redshift passes a TIMESTAMP as datetime; case_entry parses the ISO text with datetime.fromisoformat.
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_time_helpers.rb#L18
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def posix_timestamp(ts):
    from datetime import datetime
    if not ts:
      return None
    return (ts - datetime(1970, 1, 1)).total_seconds()


def case_entry(ts):
    from datetime import datetime
    return posix_timestamp(datetime.fromisoformat(ts))
