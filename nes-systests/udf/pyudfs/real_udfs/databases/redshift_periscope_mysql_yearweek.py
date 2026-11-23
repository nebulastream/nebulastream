# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2024-03-15 14:22:05"], ["2023-12-31 23:59:59"], ["2021-01-03 08:05:00"], ["2020-02-29 00:00:00.250000"], ["2019-07-04 17:45:30"], ["2024-12-30 06:10:42"]]
# real-udf: origin=databases id=redshift_periscope_mysql_yearweek requires= wrapped=1 ported=0
# Wrapper: Redshift passes a TIMESTAMP as datetime; case_entry parses the ISO text with datetime.fromisoformat.
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_mysql_compat.rb#L101
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def mysql_yearweek(ts):
    if not ts:
      return None
    cal = ts.isocalendar()
    return str(cal[0]) + str(cal[1]).zfill(2)


def case_entry(ts):
    from datetime import datetime
    return mysql_yearweek(datetime.fromisoformat(ts))
