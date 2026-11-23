# codon-fuzz: entry=email_domain args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["jane.doe@example.com"], ["ops+alerts@mail.acme.io"], ["MUELLER@Firma.DE"], ["no-at-sign"], ["a@b@c.org"]]
# real-udf: origin=databases id=redshift_periscope_email_domain requires= wrapped=0 ported=0
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L22
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def email_domain(email):
    if not email:
      return None
    return email.split('@')[-1]
