# codon-fuzz: entry=traffic_type args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["http://www.google.com/search?q=darwin+beagle+plants"], ["-"], ["Unknown"], ["https://t.co/AbC123"], ["http://www.darwinsbeagleplants.org/gallery.htm"], ["https://notgoogle.com/"], ["https://uk.search.yahoo.com/"], ["http://www.reddit.com/r/botany/"]]
# real-udf: origin=github-spark id=w3c_traffic_type requires= wrapped=0 ported=0
# Source: https://github.com/AhmedIkram05/w3c-etl-pipeline/blob/9b30e751e8f4dadd4c9eca2e26628b7f30a123bd/pipeline/spark/jobs/utils/transformations.py#L122-L147
# Repository: AhmedIkram05/w3c-etl-pipeline @ commit 9b30e751e8f4dadd4c9eca2e26628b7f30a123bd
# License: MIT
# Copied verbatim from pipeline/spark/jobs/utils/transformations.py (L69-L71, L99-L119, L122-L147); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

from urllib.parse import urlparse


_SEARCH_ENGINES = ["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex", "ask"]
_SOCIAL_DOMAINS = ["facebook", "twitter", "linkedin", "reddit", "t.co", "instagram"]
_INTERNAL_DOMAINS = ["darwinsbeagleplants", "134.36.36.75"]


def _extract_domain(referrer_url: str) -> str:
    """Extract domain from a referrer URL (plain function, not a UDF).

    Used by ``traffic_type`` which cannot call UDFs from inside a UDF body
    (no SparkContext available on executor workers).
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
        # Strip only a LEADING 'www.' label (see referrer_domain above).
        host = parsed.netloc.split("@")[-1].split(":")[0].lower()
        domain = host[4:] if host.startswith("www.") else host
        return domain or referrer_url
    except Exception:
        return str(referrer_url)


def traffic_type(referrer_url):
    """Classify a referrer into a traffic-source category.

    Categories: ``Direct``, ``Internal``, ``Search Engine``,
    ``Social``, ``Referral``, ``Unknown``.
    """
    if not referrer_url or referrer_url.strip() == "-":
        return "Direct"
    if referrer_url == "Unknown":
        return "Unknown"

    domain = _extract_domain(referrer_url)

    # Domain-boundary match: a pattern only matches whole dot-delimited
    # labels of the domain, so 'notgoogle.com' is not a Google referrer.
    # Wrapping the domain in dots lets one substring check express that
    # boundary for every pattern, including multi-label ones like 't.co'.
    padded = f".{domain}."
    if any(f".{s}." in padded for s in _INTERNAL_DOMAINS):
        return "Internal"
    if any(f".{s}." in padded for s in _SEARCH_ENGINES):
        return "Search Engine"
    if any(f".{s}." in padded for s in _SOCIAL_DOMAINS):
        return "Social"
    return "Referral"
