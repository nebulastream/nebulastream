# codon-fuzz: entry=is_valid_email args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["john.doe@example.com"], ["jane_doe+newsletter@mail.co.uk"], ["invalid.email@"], ["no-at-sign.example.com"], ["user@localhost"], ["first.last@sub.domain.org"], ["bad@domain.c"]]
# real-udf: origin=github-spark id=irfan_is_valid_email requires= wrapped=0 ported=0
# Source: https://github.com/mohamedirfankader/databricks-code-repo/blob/26629e93afffc4a9c1489f820fd020a49671c9b7/10_Databricks_Azure_Usecases/cdc_cdf_scd/utilities/utils.py#L5-L14
# Repository: mohamedirfankader/databricks-code-repo @ commit 26629e93afffc4a9c1489f820fd020a49671c9b7
# License: Apache-2.0
# Copied verbatim from 10_Databricks_Azure_Usecases/cdc_cdf_scd/utilities/utils.py (L5-L14); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=BooleanType())

import re


def is_valid_email(email):
    """
    This function checks if the given email address has a valid format using regex.
    Returns True if valid, False otherwise.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None
