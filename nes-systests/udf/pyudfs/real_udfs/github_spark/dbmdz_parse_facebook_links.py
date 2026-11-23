# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.nytimes.com%2F2016%2F11%2F09%2Fus%2Fpolitics%2Felection-results.html&h=AT0xYz"], ["http://www.facebook.com/l.php?u=http%3A%2F%2Fbit.ly%2F2fXyZ1q&h=ATNq3"], ["https://www.spiegel.de/politik/deutschland/merkel-a-1120345.html"], ["https://twitter.com/search?q=%23bundestag&src=hash"], ["https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.zeit.de%2Fpolitik%2F&u=https%3A%2F%2Fexample.org%2F"], ["https://www.facebook.com/events/1234567890/"]]
# real-udf: origin=github-spark id=dbmdz_parse_facebook_links requires= wrapped=1 ported=0
# Wrapper: the UDF returns the input text, or the list of u= values for a redirect link;
# Wrapper: case_entry joins a list with '|'.
# Source: https://github.com/dbmdz/webarchiv-dh-derivate/blob/856adf8ff9b75424a49c20c7a277fa19dce4d6c1/extract/social_media_util.py#L163-L169
# Repository: dbmdz/webarchiv-dh-derivate @ commit 856adf8ff9b75424a49c20c7a277fa19dce4d6c1
# License: MIT
# Copied verbatim from extract/social_media_util.py (L163-L169); only the module's own import lines that the code uses are prepended.

from urllib.parse import urlparse, parse_qs, unquote


def parse_facebook_links(dest: str) -> str:
    query = urlparse(dest).query
    if query is not None:
        parsed_query = parse_qs(query)
        if "u" in parsed_query.keys():
            dest = parsed_query["u"]
    return dest


def case_entry(dest):
    result = parse_facebook_links(dest)
    return result if isinstance(result, str) else "|".join(result)
