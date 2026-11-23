# codon-fuzz: entry=url_param args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://www.example.com/search?q=running+shoes&page=2", "q"], ["https://www.example.com/search?q=running+shoes&page=2", "page"], ["https://shop.io/?utm_source=newsletter&utm_campaign=spring%20sale", "utm_campaign"], ["https://example.com/?id=1&id=2", "id"], ["http://tracker.net/p.gif?uid=8f3a2c&ref=https%3A%2F%2Fgoogle.com%2F", "ref"]]
# real-udf: origin=databases id=redshift_periscope_url_param requires= wrapped=0 ported=1
# Ported: Python 2 urlparse module replaced with urllib.parse (the same functions in Python 3).
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L106
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def url_param(url, param):
    import urllib.parse as urlparse
    if not url:
      return None
    try:
      u = urlparse.urlparse(url)
      return urlparse.parse_qs(u.query)[param][0]
    except KeyError:
      return None
