# codon-fuzz: entry=normalize_name args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["John O'Connor"], ["  MARY-ANNE   Smith "], ["José García"], ["Dr. Robert J. Brown, Jr."], ["Acme Corp. (UK) Ltd"]]
# real-udf: origin=github-spark id=packt_mdm_normalize_name requires= wrapped=0 ported=0
# Source: https://github.com/PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI/blob/08ada2f425725e261879697275a394113f87ee61/chapter11/notebooks/electroniz_gold_customer_mdm.py#L37-L44
# Repository: PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI @ commit 08ada2f425725e261879697275a394113f87ee61
# License: MIT
# Copied verbatim from chapter11/notebooks/electroniz_gold_customer_mdm.py (L37-L44); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(StringType())

import re


def normalize_name(name: str) -> str:
    if not name:
        return None
    n = name.lower()
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n
