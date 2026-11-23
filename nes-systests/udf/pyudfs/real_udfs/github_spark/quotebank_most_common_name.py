# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["[\"Donald Trump\", \"Donald Trump\", \"Hillary Clinton\"]"], ["[\"Barack Obama\"]"], ["[\"Bernie Sanders\", \"Joe Biden\", \"Joe Biden\", \"Bernie Sanders\"]"], ["[\"Angela Merkel\", \"Emmanuel Macron\", \"Boris Johnson\"]"], ["[\"Elon Musk\", \"elon musk\", \"Elon Musk\"]"]]
# real-udf: origin=github-spark id=quotebank_most_common_name requires= wrapped=1 ported=0
# Wrapper: the UDF takes an array of names; case_entry reads it from a JSON array.
# Source: https://github.com/epfl-dlab/Quotebank/blob/a203acf5a05e34841671578d14d6a0be6a66a3ef/quobert/dataprocessing/postprocessing/process_res.py#L52-L54
# Repository: epfl-dlab/Quotebank @ commit a203acf5a05e34841671578d14d6a0be6a66a3ef
# License: MIT
# Copied verbatim from quobert/dataprocessing/postprocessing/process_res.py (L52-L54); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf

from collections import Counter


def get_most_common_name(names):
    return sorted(Counter(names).items(), key=lambda x: (-x[1], x[0]))[0][0]


import json


def case_entry(names_json):
    return get_most_common_name(json.loads(names_json))
