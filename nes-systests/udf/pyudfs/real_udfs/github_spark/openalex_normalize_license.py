# codon-fuzz: entry=normalize_license args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["https://creativecommons.org/licenses/by-nc-nd/4.0/"], ["CC BY 4.0"], ["http://creativecommons.org/publicdomain/zero/1.0/"], ["info:eu-repo/semantics/openAccess"], ["GPL-3.0-or-later"], ["Apache-2.0"], ["Open Government Licence v3.0"], ["https://pubs.acs.org/page/policy/authorchoice_termsofuse.html"], ["CC BY-SA 3.0"]]
# real-udf: origin=github-spark id=openalex_normalize_license requires= wrapped=0 ported=0
# Source: https://github.com/ourresearch/openalex-walden/blob/d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f/libraries/dlt_utils/openalex/dlt/normalize.py#L230-L299
# Repository: ourresearch/openalex-walden @ commit d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f
# License: MIT
# Copied verbatim from libraries/dlt_utils/openalex/dlt/normalize.py (L230-L299); only the module's own import lines that the code uses are prepended.
# Applied per value through: spark-pandas_udf

def normalize_license(text):
    if not text:
        return None

    normalized_text = text.replace(" ", "").replace("-", "").lower()

    license_lookups = [
        # publisher-specific
        ("elsevier.com/openaccess/userlicense", None),
        ("pubs.acs.org/page/policy/authorchoice_termsofuse.html", "other-oa"),
        # oxjob #1228: a publisher's own licence for its paid open access option, registered only on OA articles,
        # counts as open (help.openalex.org Licenses: publisher-specific open licences, like ACS AuthorChoice above).
        # IEEE OAPA.html (IEEE.html, the copyright transfer, stays unmapped); Optica #VOR-OA (#VOR, #AM, #GOV and
        # #OPG-OO stay unmapped). Keep in step with the copies in PDF.py and RepoBackfill.py (oxjob #991).
        ("licenseinformation/oapa.html", "other-oa"),
        ("10.1364/oa_license_v1#voroa", "other-oa"),
        ("10.1364/oa_license_v2#voroa", "other-oa"),
        ("arxiv.orgperpetual", "other-oa"),
        ("arxiv.orgnonexclusive", "other-oa"),
        # oxjob #1169: the UK Open Government Licence (nationalarchives.gov.uk/doc/open-government-licence) and the
        # Open Government Licence - Canada (open.canada.ca/en/open-government-licence-canada) are open licences like
        # CC BY; Crown-copyright articles and Natural Resources Canada's GEOSCAN reports (10.4095) carry them.
        ("opengovernmentlicence", "other-oa"),
        ("opengovernmentlicense", "other-oa"),

        # creative Commons licenses
        # Text form ("CC BY-NC-ND 4.0") and URL form
        # ("creativecommons.org/licenses/by-nc-nd/4.0") differ once hyphens are
        # stripped, so each flavor needs both keys. Longest-first.
        ("ccbyncnd", "cc-by-nc-nd"),
        ("licenses/byncnd", "cc-by-nc-nd"),
        ("ccbyncsa", "cc-by-nc-sa"),
        ("licenses/byncsa", "cc-by-nc-sa"),
        ("ccbynd", "cc-by-nd"),
        ("licenses/bynd", "cc-by-nd"),
        ("ccbysa", "cc-by-sa"),
        ("licenses/bysa", "cc-by-sa"),
        ("ccbync", "cc-by-nc"),
        ("licenses/bync", "cc-by-nc"),
        ("ccby", "cc-by"),
        # plain BY needs a trailing boundary, or a malformed flavor
        # (licenses/by-n345667c-nd) resolves to the most permissive answer.
        ("creativecommons.org/licenses/by/", "cc-by"),
        ("licenses/by/", "cc-by"),
        ("licenses/by4", "cc-by"),
        ("licenses/by3", "cc-by"),
        ("licenses/by2", "cc-by"),
        ("licenses/by1", "cc-by"),

        # public domain
        ("publicdomain", "public-domain"),

        # open Access patterns
        ("infoeureposematicsaccess", "other-oa"),
        ("openaccess", "other-oa"),

        # software/Dataset licenses
        ("mit ", "mit"),
        ("gpl3", "gpl-3"),
        ("gpl2", "gpl-2"),
        ("gpl", "gpl"),
        ("apache2", "apache-2.0")
    ]

    for lookup, license in license_lookups:
        if lookup in normalized_text:
            if license == "public-domain" and "worksnotinthepublicdomain" in normalized_text:
                continue
            return license
    return None
