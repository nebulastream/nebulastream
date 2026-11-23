# codon-fuzz: entry=expand_address args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["123 Main St."], ["45 Oak Av #12"], ["9 Elm dr"], ["77 Sunset Rd Ap 3"], ["5 Maple Ln"], ["  1600 Pennsylvania Av NW  "], ["221B Baker St"]]
# real-udf: origin=github-spark id=packt_mdm_expand_address requires= wrapped=0 ported=0
# Source: https://github.com/PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI/blob/08ada2f425725e261879697275a394113f87ee61/chapter11/notebooks/electroniz_gold_customer_mdm.py#L58-L68
# Repository: PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI @ commit 08ada2f425725e261879697275a394113f87ee61
# License: MIT
# Copied verbatim from chapter11/notebooks/electroniz_gold_customer_mdm.py (L49-L56, L58-L68); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(StringType())

import re


ABBREV = {
    r"\bst\.?\b":  "Street",
    r"\bav\.?\b":  "Avenue",
    r"\bdr\.?\b":  "Drive",
    r"\brd\.?\b":  "Road",
    r"\bln\.?\b":  "Lane",
    r"\bap\.?\b":  "Apartment",
}


def expand_address(addr: str) -> str:
    if not addr:
        return None
    a = addr.strip()
    for pattern, replacement in ABBREV.items():
        a = re.sub(pattern, replacement, a, flags=re.IGNORECASE)
    # Remove hash/pound sign
    a = a.replace("#", " ")
    a = re.sub(r"\s+", " ", a).strip()
    return a
