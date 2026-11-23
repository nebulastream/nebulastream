# codon-fuzz: entry=case_entry args=INT64,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[1700000000, "s"], [1700000000123, "ms"], [1700000000123456, "us  "], [19700, "D"], [0, "s"], [1609459200, "s "]]
# real-udf: origin=databases id=redshift_aws_f_unixts_to_timestamp requires=pandas wrapped=1 ported=0
# Wrapper: The UDF returns a pandas Timestamp; case_entry returns it as str().
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_unixts_to_timestamp%28bigint%2Cvarchar%29/function.sql#L6
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_unixts_to_timestamp(ts, units):
    import pandas
    return pandas.to_datetime(ts, unit=units.rstrip())


def case_entry(ts, units):
    return str(f_unixts_to_timestamp(ts, units))
