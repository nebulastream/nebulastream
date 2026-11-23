# codon-fuzz: entry=f_parse_url args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://www.example.com/products/shoes?color=red&size=42#reviews", "netloc"], ["https://www.example.com/products/shoes?color=red&size=42#reviews", "path"], ["http://api.shop.io:8080/v2/orders?id=10422&expand=items", "query"], ["ftp://files.example.org/pub/data/2024.csv", "scheme"], ["https://docs.python.org/3/library/urllib.parse.html#url-parsing", "fragment"], ["https://User:Pw@Sub.Domain.EXAMPLE.com:443/login", "hostname"]]
# real-udf: origin=databases id=redshift_aws_f_parse_url requires= wrapped=0 ported=1
# Ported: Python 2 urlparse module replaced with urllib.parse (the same functions in Python 3).
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_parse_url%28varchar%2Cvarchar%29/function.sql#L10
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_parse_url(url, part):
    if url is None:
        return None
    else:
        import urllib.parse as urlparse
        parsed = urlparse.urlparse(url)
        return getattr(parsed, part)
