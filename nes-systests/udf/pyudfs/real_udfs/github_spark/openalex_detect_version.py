# codon-fuzz: entry=detect_version_from_metadata args=VARSIZED,VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["<dc:type>info:eu-repo/semantics/acceptedVersion</dc:type>", "oai:eprints.whiterose.ac.uk:12345"], ["<dc:description>Peer reviewed</dc:description><dc:type>article</dc:type>", "oai:repository.tudelft.nl:uuid:1a2b"], ["<dc:type>info:eu-repo/semantics/publishedVersion</dc:type>", "oai:zenodo.org:4567890"], ["<dc:type>Preprint</dc:type><dc:title>On graphs</dc:title>", "oai:arXiv.org:2101.00001"], ["<dc:type>article</dc:type>", "oai:shura.shu.ac.uk:28761"], ["", "oai:hal.science:hal-01234567"], ["Version of the article as published by the journal", "oai:dspace.mit.edu:1721.1/12345"]]
# real-udf: origin=github-spark id=openalex_detect_version requires= wrapped=0 ported=0
# Source: https://github.com/ourresearch/openalex-walden/blob/d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f/notebooks/ingest/Repo.py#L159-L218
# Repository: ourresearch/openalex-walden @ commit d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f
# License: MIT
# Copied verbatim from notebooks/ingest/Repo.py (L159-L218); only the module's own import lines that the code uses are prepended.

import re


def detect_version_from_metadata(metadata_string, native_id):
    """
    Detect version from stringified metadata and native_id based on regex patterns
    Returns 'acceptedVersion', 'publishedVersion', or 'submittedVersion'
    """

    ACCEPTED_VERSION_REPOS = [
        "oai:catalog.lib.kyushu-u.ac.jp",
        "oai:cronfa.swan.ac.uk",
        "oai:dora",
        "oai:e-space.mmu.ac.uk",
        "oai:hrcak.srce.hr",
        "oai:infocom.co.jp",
        "oai:library.wur.nl",
        "oai:lirias2repo.kuleuven.be",
        "oai:mro.massey.ac.nz",
        "oai:raumplan.iaus.ac.rs",
        "oai:repository.arizona.edu",
        "oai:repository.cardiffmet.ac.uk",
        "oai:researchbank.swinburne.edu.au",
        "oai:researchonline.gcu.ac.uk",
        "oai:rke.abertay.ac.uk",
        "oai:shura.shu.ac.uk",
        "oai:taju.uniarts.fi"
    ]

    if native_id:
        native_id_str = str(native_id)
        for repo in ACCEPTED_VERSION_REPOS:
            if native_id_str.startswith(repo + ":"):
                return "acceptedVersion"

    if not metadata_string:
        return "submittedVersion"

    search_text = str(metadata_string).lower()

    accepted_patterns = [
        r"accepted.?version",
        r"version.?accepted", 
        r"accepted.?manuscript",
        r"peer.?reviewed",
        r"refereed/peer-reviewed"
    ]

    for pattern in accepted_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "acceptedVersion"

    published_patterns = [
        r"publishedversion",
        r"published.*version",
        r"version.*published"
    ]

    for pattern in published_patterns:
        if re.search(pattern, search_text, re.IGNORECASE | re.MULTILINE | re.DOTALL):
            return "publishedVersion"

    return "submittedVersion"
