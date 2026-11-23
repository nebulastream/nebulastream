# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["CC BY-NC-ND 4.0"], ["cc_by_4"], ["Creative Commons Attribution 4.0 International"], ["Atribución-NoComercial-SinDerivadas 3.0 España"], ["Creative Commons - Namensnennung - Weitergabe unter gleichen Bedingungen 4.0 International"], ["CC Zero"], ["by-nc-sa"], ["© 2021 The Author(s). This is an open access article under the CC BY-NC licence."], ["Attribution-NonCommercial 4.0 International"]]
# real-udf: origin=github-spark id=openalex_cc_licence_from_rights requires= wrapped=1 ported=0
# Wrapper: the UDF takes the record's array of dc:rights values; case_entry passes one value.
# Source: https://github.com/ourresearch/openalex-walden/blob/d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f/notebooks/ingest/Repo.py#L411-L417
# Repository: ourresearch/openalex-walden @ commit d727817db8c1bfa5fae5bec4103dd8bd07ce3f2f
# License: MIT
# Copied verbatim from notebooks/ingest/Repo.py (L325-L325, L327-L410, L411-L417); only the module's own import lines that the code uses are prepended.
# Applied per value through: spark-pandas_udf

import re
import unicodedata


RIGHTS_RESTRICTED_PATTERN = r"restrictedaccess|closedaccess|embargoedaccess|rightsstatements\.org/vocab/inc|incopyright"


# --- cc licence block (oxjob #2516): start. RepoBackfill.py holds the same block, byte for byte ---
# oxjob #2516: a Creative Commons licence a dc:rights value states without a creativecommons.org URL. EPrints exports
# its licence codes (cc_by_4, cc_by_nc_nd, cc_by_nc_sa_4_0: underscores, which normalize_license never matched), and
# many repositories write the licence in words ("Creative Commons Attribution 4.0 International", "Atribución-
# NoComercial-SinDerivadas 3.0 España", "Creative Commons - Namensnennung 4.0"), or after a copyright line. Pure Python;
# notebooks/ingest/tests/test_repo_cc_licence.py runs it and checks RepoBackfill.py holds the same block.
# Read in this order: a CC code or abbreviation (cc by-nc-nd, cc_by_4, CCBYNC, spdx CC-BY-SA-4.0); the EPrints CC0 codes;
# the licence's name in words. The name must follow "Creative Commons" or "CC" in the value, or be the whole value
# ("Attribution-NonCommercial 4.0 International"). Its elements are read only as a chain right after the Attribution
# element, so prose after the name ("... Attribution License, which permits commercial and non-commercial use") adds
# nothing. No Attribution element, or ND with SA, names no licence ("Creative Commons", "Title 17 ... unless on a
# Creative Commons license"). Licences that reuse the BY-NC-ND letters (KOGL BY-NC-ND) never match: the letters must
# follow "CC" or "Creative Commons".
# A dc:rights value that restricts access keeps a licence read here off the record: RIGHTS_RESTRICTED_PATTERN plus the
# COAR closed / restricted / embargoed URIs and "metadata only", tested on the same form (lower case, no spaces/hyphens).
CC_RIGHTS_BLOCK_PATTERN = RIGHTS_RESTRICTED_PATTERN + r"|coar/access_right/c_(?:14cb|16ec|f1cf)|metadataonly"
# A prompt to pick a licence names none: "Bitte wählen Sie eine Lizenz aus: (Unsere Empfehlung: CC-BY)" (Bremen).
CC_PROMPT_RE = re.compile(r"w[äa]hlen sie|empfehlung|please (?:choose|select)|we recommend|recommended licen")
# "cc_by_og" is not CC BY: DERA's EPrints code for the UK Open Government Licence (its pages say so).
CC_CODE_RE = re.compile(r"(?<![a-z0-9])cc[\s:-]*by((?:[\s/-]*(?:nc|nd|sa)(?![a-z]))*)(?![a-z])(?!\s*og(?![a-z]))")
CC_NAMED_BY_RE = re.compile(r"creative[\s-]*commons?[\s:-]*(?:licen[cs]e[\s:-]*)?(?:«\s*)?by((?:[\s/-]*(?:nc|nd|sa)(?![a-z]))*)(?![a-z])")
CC_ZERO_RE = re.compile(r"^cc[\s-]*(?:zero|public[\s-]*domain)$")
# A whole value that is only the letters: "by-nc-nd", "BY-SA 4.0".
CC_BARE_LETTERS_RE = re.compile(r"^by((?:[\s/-]+(?:nc|nd|sa))+)(?:\s*v?\d(?:\.\d)?)?$")
CC_CONTEXT_RE = re.compile(r"creative[\s-]*commons?|(?<![a-z])(?<!data )commons attribution|クリエイティブ・?コモンズ|(?<![a-z0-9])cc(?![a-z0-9])")
_CC_BY = (r"attribution|atribuci[oó]n|reconocimiento|atribui[cç][aã]o|reconeixement|namensnennung|uznanie autorstwa"
          r"|naamsvermelding|attribuzione|paternit[eé]|nimeä|erkännande|navngivelse|uznání autorství")
_CC_NC = (r"non[\s-]?commerc|no[\s-]?comercial|n[ãa]o[\s-]?comercial|nicht[\s-]?kommerz|keine kommerzielle nutzung"
          r"|niekomercyjn|niet[\s-]?commercieel|pas d.utilisation commerciale|非営利|epäkaupallinen|ickekommersiell"
          r"|ikke[\s-]?kommerciel|nekomerční")
_CC_ND = (r"no[\s-]?deriv|sin[\s-]?(?:obras?[\s-]?)?deriva|no[\s-]?derivadas|sem[\s-]?(?:obras?[\s-]?)?deriva"
          r"|vedada a cria[cç][aã]o de obras derivadas|sense[\s-]?obr\w*[\s-]?deriva|keine[\s-]?bearbeitung"
          r"|bez utworów zależnych|geen[\s-]?afgeleide|non[\s-]?opere[\s-]?derivate|pas de modification|改変禁止"
          r"|ei muutoksia|inga bearbetningar|ingen bearbejdelse")
_CC_SA = (r"share[\s-]?alike|compartir[\s-]?igual|compartilha[\s-]?igual|weitergabe unter gleichen(?: bedingungen)?"
          r"|na tych samych warunkach|gelijk[\s-]?delen|condividi allo stesso modo|partage dans les m[êe]mes|継承"
          r"|jaa samoin|dela lika")
CC_PARTS = (("nc", re.compile(_CC_NC)), ("nd", re.compile(_CC_ND)), ("sa", re.compile(_CC_SA)))
_CC_CHAIN = (r"(?:" + _CC_BY + r"|(?<=コモンズ)\s*表示|(?<=コモンズ・)表示)((?:[\s,.:;–—‐・/-]*(?:uso\s+|u[żz]ycie\s+)?(?:"
             + _CC_NC + "|" + _CC_ND + "|" + _CC_SA + r")\w*)*)")
CC_CHAIN_RE = re.compile(_CC_CHAIN)
# The whole value is the licence name: "Attribution", "Attribution-NonCommercial-NoDerivatives 4.0 International",
# "Atribución-NoComercial-SinDerivadas 3.0 España", "Reconocimiento – NoComercial – SinObraDerivada".
CC_WHOLE_NAME_RE = re.compile(r"^(?:licen[cs]e:?\s*|udzielam licencji\.?\s*)?" + _CC_CHAIN.replace("((?:", "(?:(?:", 1)
                              + r"(?:\s*v?\d(?:\.\d)?(?:\s+[^\W\d]+){0,3}"
                              r"|\s+(?:international|internacional|internationale?|unported|generic|licen[cs]e))?\.?$")


def _cc_flavour(parts):
    """{'nc', 'nd', 'sa'} -> 'cc-by-nc-nd' etc. (every licence here has BY); None for ND with SA."""
    if "nd" in parts and "sa" in parts:
        return None
    return "-".join(["cc-by"] + [p for p in ("nc", "nd", "sa") if p in parts])


def cc_licence_from_text(value):
    """The Creative Commons licence one dc:rights value names in a code or in words, else None."""
    if not value:
        return None
    t = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value).lower().replace("_", " ")).strip()
    if CC_PROMPT_RE.search(t):
        return None
    m = CC_CODE_RE.search(t)
    if m:
        return _cc_flavour(set(re.findall(r"nc|nd|sa", m.group(1))))
    if CC_ZERO_RE.match(t):
        return "public-domain"
    m = CC_BARE_LETTERS_RE.match(t)
    if m:
        return _cc_flavour(set(re.findall(r"nc|nd|sa", m.group(1))))
    if CC_CONTEXT_RE.search(t):
        m = CC_NAMED_BY_RE.search(t)
        if m:
            return _cc_flavour(set(re.findall(r"nc|nd|sa", m.group(1))))
        m = CC_CHAIN_RE.search(t)
    elif CC_WHOLE_NAME_RE.match(t):
        m = CC_CHAIN_RE.search(t)
    else:
        return None
    if not m:
        return None
    return _cc_flavour({p for p, rx in CC_PARTS if rx.search(m.group(1))})


def cc_licence_from_rights(values):
    """The licence of the first dc:rights value that names a Creative Commons licence (see above), else None."""
    for v in values or []:
        lic = cc_licence_from_text(v)
        if lic:
            return lic
    return None


def case_entry(rights):
    return cc_licence_from_rights([rights])
