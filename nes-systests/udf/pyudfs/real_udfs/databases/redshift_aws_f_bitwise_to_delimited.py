# codon-fuzz: entry=f_bitwise_to_delimited args=INT64,INT64,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[5, 8, ","], [255, 8, "|"], [1024, 16, ","], [0, 4, ";"], [3, 2, "-"], [100000, 12, ","]]
# real-udf: origin=databases id=redshift_aws_f_bitwise_to_delimited requires= wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_bitwise_to_delimited%28bigint%2Cint%2Cchar%29/function.sql#L14
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_bitwise_to_delimited(bitwise_column, bits_in_column, delimiter):
    # Convert column to binary, strip "0b" prefix, pad out with zeroes
    b = bin(bitwise_column)[2:].zfill(bits_in_column)
    # Convert each character to a member of an array, join array into string using delimiter
    o = delimiter.join([b[i:i+1] for i in range(0, len(b), 1)])
    return o
