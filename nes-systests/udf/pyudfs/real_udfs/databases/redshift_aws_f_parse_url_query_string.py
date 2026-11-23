# codon-fuzz: entry=f_parse_url_query_string args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://www.example.com/search?q=running+shoes&page=2&sort=price_asc"], ["https://shop.io/cart?utm_source=newsletter&utm_medium=email&utm_campaign=spring_sale"], ["https://example.com/path/only"], ["http://tracker.net/p.gif?uid=8f3a2c&ev=view&ts=1700000000&ref=https%3A%2F%2Fgoogle.com%2F"], ["https://maps.example.com/?lat=52.5200&lng=13.4050&zoom=12"], ["https://example.com/?a=1&a=2&b="]]
# real-udf: origin=databases id=redshift_aws_f_parse_url_query_string requires= wrapped=0 ported=1
# Ported: Python 2 urlparse module replaced with urllib.parse (the same functions in Python 3).
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_parse_url_query_string%28varchar%29/function.sql#L7
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_parse_url_query_string(url):
    from urllib.parse import urlparse, parse_qsl
    import json
    return json.dumps(dict(parse_qsl(urlparse(url)[4])))
