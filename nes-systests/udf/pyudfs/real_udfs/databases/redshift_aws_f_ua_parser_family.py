# codon-fuzz: entry=f_ua_parser_family args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"], ["Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"], ["Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"], ["curl/8.4.0"], ["Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"], ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.91"]]
# real-udf: origin=databases id=redshift_aws_f_ua_parser_family requires=ua-parser wrapped=0 ported=0
# Source: https://github.com/awslabs/amazon-redshift-udfs/blob/16fc7a773592315b41a4bd0b3711c26bd2fff51a/python-udfs/f_ua_parser_family%28varchar%29/function.sql#L7
# Commit: 16fc7a773592315b41a4bd0b3711c26bd2fff51a
# License: Apache-2.0
# The original body is embedded in SQL ($$...$$); it is wrapped here in a def with the declared SQL parameters.
# Removed from master in 2026 (Redshift Python UDF deprecation); pinned to the last commit that has it.

def f_ua_parser_family(ua):
    from ua_parser import user_agent_parser
    return user_agent_parser.ParseUserAgent(ua)['family']
