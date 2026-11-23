# codon-fuzz: entry=url_domain args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://www.example.com/products?id=7"], ["http://blog.shop.io:8080/2024/03/post"], ["ftp://files.example.org/pub/"], ["https://user:pw@sub.domain.co.uk/login"], ["example.com/no-scheme"]]
# real-udf: origin=databases id=redshift_periscope_url_domain requires= wrapped=0 ported=1
# Ported: Python 2 urlparse module replaced with urllib.parse (the same functions in Python 3).
# Source: https://github.com/PeriscopeData/redshift-udfs/blob/d9cd02c9bbd5aec39a24996ac2548f96ffa5c381/lib/udf_string_utils.rb#L62
# Commit: d9cd02c9bbd5aec39a24996ac2548f96ffa5c381
# License: MIT
# The original body is a Python string inside a Ruby UDF definition that the harness emits as CREATE FUNCTION ... LANGUAGE plpythonu; wrapped here in a def with the declared SQL parameters.

def url_domain(url):
    from urllib.parse import urlparse
    if not url:
      return None
    try:
      u = urlparse(url)
      return u.netloc
    except ValueError:
      return None
