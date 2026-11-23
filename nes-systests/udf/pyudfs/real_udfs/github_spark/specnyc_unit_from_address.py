# codon-fuzz: entry=_extract_unit_from_address args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["245 EAST 63RD STREET, 5A"], ["100 WEST 57TH STREET, PH12A"], ["35 HUDSON YARDS, #3B"], ["1 CENTRAL PARK WEST, UNIT 2"], ["150 W 56TH ST APT 4F"], ["40 BROAD STREET, 12-C"], ["301 EAST 79TH ST, APT 22K"]]
# real-udf: origin=github-spark id=specnyc_unit_from_address requires=pandas wrapped=0 ported=0
# Source: https://github.com/ColbyRReichenbach/SPEC-NYC/blob/b6538c8b3a69098fe5c6ae1ea091b30d70396428/src/etl.py#L231-L253
# Repository: ColbyRReichenbach/SPEC-NYC @ commit b6538c8b3a69098fe5c6ae1ea091b30d70396428
# License: MIT
# Copied verbatim from src/etl.py (L197-L229, L231-L253); only the module's own import lines that the code uses are prepended.
# Applied per value through: pandas-apply

import re
import pandas as pd


_INVALID_UNIT_TOKENS = {
    "STREET", "ST", "AVENUE", "AVE", "ROAD", "RD", "BOULEVARD", "BLVD",
    "PLACE", "PL", "LANE", "LN", "DRIVE", "DR", "COURT", "CT", "TERRACE",
    "TER", "PARKWAY", "PKWY", "WAY", "NORTH", "SOUTH", "EAST", "WEST",
}

_EXPLICIT_UNIT_SUFFIX_RE = re.compile(r"(?:APT|APARTMENT|UNIT|#)\s*([A-Z0-9\-]+)\s*$", re.IGNORECASE)


def _normalize_unit_token(value: object, *, strict: bool) -> str | None:
    """Normalize apartment/unit tokens for stable property IDs."""
    if pd.isna(value):
        return None

    token = str(value).strip().upper()
    if token in {"", "NAN", "NONE", "NULL"}:
        return None

    token = re.sub(r"^(APT|APARTMENT|UNIT)\s*", "", token)
    token = token.lstrip("#").strip()
    token = re.sub(r"\s+", "", token)
    token = re.sub(r"[^A-Z0-9\-]", "", token)

    if not token or token in _INVALID_UNIT_TOKENS:
        return None

    if strict:
        has_unit_signal = bool(re.search(r"\d", token) or token.startswith("PH"))
        if not has_unit_signal:
            return None

    return token


def _extract_unit_from_address(address_value: object) -> str | None:
    """
    Extract probable unit identifier from address when apartment_number is missing.
    Common formats include ', 5A', ', PH12A', ', #3B', ', UNIT 2'.
    """
    if pd.isna(address_value):
        return None

    address = str(address_value).strip()
    if not address:
        return None

    trailing = re.search(r",\s*([^,]+)\s*$", address)
    if trailing:
        token = _normalize_unit_token(trailing.group(1), strict=True)
        if token:
            return token

    explicit = _EXPLICIT_UNIT_SUFFIX_RE.search(address)
    if explicit:
        return _normalize_unit_token(explicit.group(1), strict=True)

    return None
