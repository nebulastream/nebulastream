# codon-fuzz: entry=normalize_phone_for_match args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["+1 (415) 555-0132"], ["415.555.0132"], ["+44 20 7946 0958"], ["555-0132"], ["+91 98765 43210"], ["001-212-555-0199"]]
# real-udf: origin=github-spark id=packt_mdm_normalize_phone requires= wrapped=0 ported=0
# Source: https://github.com/PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI/blob/08ada2f425725e261879697275a394113f87ee61/chapter11/notebooks/electroniz_gold_customer_mdm.py#L181-L188
# Repository: PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI @ commit 08ada2f425725e261879697275a394113f87ee61
# License: MIT
# Copied verbatim from chapter11/notebooks/electroniz_gold_customer_mdm.py (L181-L188); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(StringType())

import re


def normalize_phone_for_match(phone: str) -> str:
    """Normalize phone to digits only for matching."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    # Return last 10 digits for US/Canada, or all digits if less
    return digits[-10:] if len(digits) >= 10 else digits
