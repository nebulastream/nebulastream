# codon-fuzz: entry=f_null_syns args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["NULL"], ["N/A"], ["Not Applicable"], ["unknown"], [""], ["Berlin"], ["0"], ["null and void"]]
# real-udf: origin=databases id=redshift_aws_f_null_syns requires= wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_null_syns%28varchar%29/function.sql#L11
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_null_syns(a):
    s = {"null", "invalid", "unknown", "n/a", "not applicable", "void", "nothing", "nonexistent", "null and void", ""}
    b = str(a).lower()
    return b in s
