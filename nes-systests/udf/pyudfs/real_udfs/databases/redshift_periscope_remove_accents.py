# codon-fuzz: entry=remove_accents args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Crème Brûlée"], ["São Paulo"], ["Zürich Hauptbahnhof"], ["naïve café"], ["Ångström"], ["plain ascii"], ["Łódź"]]
# real-udf: origin=databases id=redshift_periscope_remove_accents requires= wrapped=0 ported=1
# Ported: Python 2 byte strings: the input is already text in Python 3, so the str.decode('utf-8') line is removed, and the ASCII bytes result is decoded back to str.
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L236
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def remove_accents(str):
    import unicodedata
    if not str:
      return None
    return unicodedata.normalize('NFKD', str).encode('ASCII', 'ignore').decode('ASCII')
