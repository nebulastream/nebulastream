# codon-fuzz: entry=referrer_domain args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["http://www.google.com/search?q=darwin+beagle+plants"], ["-"], ["Unknown"], ["https://user@m.facebook.com:443/"], ["bing.com/search?q=orchids"], ["https://abcwww.com/page"], ["http://134.36.36.75/index.htm"]]
# real-udf: origin=github-spark id=w3c_referrer_domain requires= wrapped=0 ported=0
# Source: https://github.com/AhmedIkram05/w3c-etl-pipeline/blob/9b30e751e8f4dadd4c9eca2e26628b7f30a123bd/pipeline/spark/jobs/utils/transformations.py#L74-L96
# Repository: AhmedIkram05/w3c-etl-pipeline @ commit 9b30e751e8f4dadd4c9eca2e26628b7f30a123bd
# License: MIT
# Copied verbatim from pipeline/spark/jobs/utils/transformations.py (L74-L96); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

from urllib.parse import urlparse


def referrer_domain(referrer_url):
    """Extract the domain from a referrer URL.

    Returns ``'Direct'`` for empty/dash referrers, ``'Unknown'`` for the
    literal string ``'Unknown'``, or the parsed domain otherwise.
    """
    if not referrer_url or referrer_url.strip() == "-":
        return "Direct"
    if referrer_url == "Unknown":
        return "Unknown"
    try:
        candidate = referrer_url.strip()
        if "//" not in candidate:
            candidate = "http://" + candidate
        parsed = urlparse(candidate)
        # Strip only a LEADING 'www.' label — str.replace would also strip a
        # mid-label 'www.' (e.g. 'abcwww.com' -> 'abccom').
        host = parsed.netloc.split("@")[-1].split(":")[0].lower()
        domain = host[4:] if host.startswith("www.") else host
        return domain or referrer_url
    except Exception:
        return str(referrer_url)
