# codon-fuzz: entry=email_rank args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["john.smith@gmail.com"], ["Mary.Jones@Hotmail.com "], ["bob@yahoo.com"], ["alice@company.co.uk"], [""], ["support@GMAIL.COM"]]
# real-udf: origin=github-spark id=packt_mdm_email_rank requires= wrapped=0 ported=0
# Source: https://github.com/PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI/blob/08ada2f425725e261879697275a394113f87ee61/chapter11/notebooks/electroniz_gold_customer_mdm.py#L199-L205
# Repository: PacktPublishing/Building-Trusted-Data-Platforms-with-Azure-Databricks-and-GenAI @ commit 08ada2f425725e261879697275a394113f87ee61
# License: MIT
# Copied verbatim from chapter11/notebooks/electroniz_gold_customer_mdm.py (L199-L205); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(LongType())

def email_rank(email: str) -> int:
    if not email:
        return 99
    domain = email.strip().lower().split("@")[-1]
    ranks = {"gmail.com": 1, "hotmail.com": 2, "yahoo.com": 3}
    return ranks.get(domain, 4)
