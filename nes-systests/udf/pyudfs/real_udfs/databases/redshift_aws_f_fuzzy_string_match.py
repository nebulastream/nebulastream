# codon-fuzz: entry=f_fuzzy_string_match args=VARSIZED,VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Acme Corporation", "ACME Corp."], ["Jon Smith", "John Smith"], ["123 Main St", "123 Main Street"], ["Microsoft", "Apple"], ["New York Mets", "New York Meats"], ["", "anything"]]
# real-udf: origin=databases id=redshift_aws_f_fuzzy_string_match requires=thefuzz wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_fuzzy_string_match%28varchar%2Cvarchar%29/function.sql#L7
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_fuzzy_string_match(string_a, string_b):
    from thefuzz import fuzz 

    return fuzz.ratio (string_a,string_b)
