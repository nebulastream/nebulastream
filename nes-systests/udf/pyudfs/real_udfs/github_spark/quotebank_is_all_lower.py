# codon-fuzz: entry=is_all_lower args=VARSIZED returns=BOOLEAN
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[QUOTE] said the [MASK] on tuesday"], ["[MASK] told reporters that [QUOTE]"], ["we will win this election, [TARGET_QUOTE]"], ["The President said [QUOTE]"], ["[MASK] [QUOTE]"]]
# real-udf: origin=github-spark id=quotebank_is_all_lower requires= wrapped=0 ported=0
# Source: https://github.com/epfl-dlab/Quotebank/blob/a203acf5a05e34841671578d14d6a0be6a66a3ef/quobert/dataprocessing/preprocessing/extract_entities.py#L200-L203
# Repository: epfl-dlab/Quotebank @ commit a203acf5a05e34841671578d14d6a0be6a66a3ef
# License: MIT
# Copied verbatim from quobert/dataprocessing/preprocessing/extract_entities.py (L200-L203); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(returnType=BooleanType())

import re


def is_all_lower(masked_text):
    text = re.sub(r'(\[MASK\]|\[QUOTE\]|\[TARGET_QUOTE\])', "", masked_text)
    return text == text.lower()
