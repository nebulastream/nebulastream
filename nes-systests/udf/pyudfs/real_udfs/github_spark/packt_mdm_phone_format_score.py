# codon-fuzz: entry=phone_format_score args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["+1 (415) 555-0132"], ["+1 415 555 0132"], ["(415) 555-0132"], ["415-5550132"], ["4155550132"], ["+44 2079 460958"], ["+44 20 7946 0958"], ["+91 98765 43210"], ["555-01"], ["+49 30 901820"]]
# real-udf: origin=github-spark id=packt_mdm_phone_format_score requires= wrapped=0 ported=0
# Source: https://github.com/PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI/blob/08ada2f425725e261879697275a394113f87ee61/chapter11/notebooks/electroniz_gold_customer_mdm.py#L86-L178
# Repository: PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI @ commit 08ada2f425725e261879697275a394113f87ee61
# License: MIT
# Copied verbatim from chapter11/notebooks/electroniz_gold_customer_mdm.py (L86-L178); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(LongType())

import re


def phone_format_score(phone: str) -> int:
    """
    Return phone formatting quality score based on country detection and format:
    5 = Perfect country-specific format (e.g., +1 (XXX) XXX-XXXX, +44 XXXX XXXXXX, +91 XXXXX XXXXX)
    4 = Has country code prefix (+1, +44, +91, etc.)
    3 = Has formatting chars but no country code
    2 = Has some formatting (parentheses, hyphens, spaces)
    1 = Just digits (unformatted)
    0 = Invalid (< 7 digits or null)
    """
    if not phone:
        return 0

    phone_stripped = phone.strip()
    digits = re.sub(r"\D", "", phone)

    # Invalid: less than 7 digits
    if len(digits) < 7:
        return 0

    # Count formatting characters (+, parentheses, hyphens, spaces)
    format_chars = len(re.findall(r"[+()\-\s]", phone))
    has_plus = phone_stripped.startswith("+")

    # US/Canada: 10 digits or 11 digits starting with 1
    if re.match(r"^1?\d{10}$", digits):
        if len(digits) == 11 and digits[0] != "1":
            pass  # Not US/Canada pattern
        elif len(digits) == 10 and digits[0] not in "23456789":
            pass  # Not US/Canada pattern
        else:
            # Valid US/Canada number
            # Check for perfect format: +1 (XXX) XXX-XXXX
            if re.match(r"^\+1\s?\(\d{3}\)\s?\d{3}-\d{4}$", phone_stripped):
                return 5
            # Has +1 prefix
            if has_plus and "1" in phone_stripped[:3]:
                return 4
            # Has formatting but no country code
            if format_chars >= 2:
                return 3
            if format_chars >= 1:
                return 2
            # Just digits
            return 1

    # UK: 12 digits starting with 44, or 11 digits starting with 0
    if re.match(r"^44\d{10}$", digits) or re.match(r"^0\d{10}$", digits):
        # Check for perfect format: +44 XXXX XXXXXX
        if re.match(r"^\+44\s\d{4}\s\d{6}$", phone_stripped):
            return 5
        # Has +44 prefix
        if has_plus and "44" in phone_stripped[:4]:
            return 4
        # Has formatting but no country code
        if format_chars >= 2:
            return 3
        if format_chars >= 1:
            return 2
        # Just digits
        return 1

    # India: 12 digits starting with 91, or 10 digits starting with [6-9]
    if re.match(r"^91\d{10}$", digits) or re.match(r"^[6-9]\d{9}$", digits):
        # Check for perfect format: +91 XXXXX XXXXX
        if re.match(r"^\+91\s\d{5}\s\d{5}$", phone_stripped):
            return 5
        # Has +91 prefix
        if has_plus and "91" in phone_stripped[:4]:
            return 4
        # Has formatting but no country code
        if format_chars >= 2:
            return 3
        if format_chars >= 1:
            return 2
        # Just digits
        return 1

    # Unknown country (>= 7 digits, doesn't match above patterns)
    if len(digits) >= 7:
        # Has + prefix (generic international format)
        if has_plus:
            return 4
        # Has formatting
        if format_chars >= 2:
            return 3
        if format_chars >= 1:
            return 2
        # Just digits
        return 1

    return 0
