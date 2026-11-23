# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["<python><pandas>"], ["<java><spring-boot><hibernate>"], ["<c++>"], [""], ["<apache-spark><pyspark><dataframe><udf>"]]
# real-udf: origin=github-spark id=so_extract_tags requires= wrapped=1 ported=0
# Wrapper: the UDF returns an array of tags; case_entry returns it as JSON text.
# Source: https://github.com/rehanalfarizu/stackoverflow-analytics-pyspark-nlp/blob/3385401530ef2db07e3727550aff00214e74f292/src/etl/data_transformer.py#L38-L44
# Repository: rehanalfarizu/stackoverflow-analytics-pyspark-nlp @ commit 3385401530ef2db07e3727550aff00214e74f292
# License: MIT
# Copied verbatim from src/etl/data_transformer.py (L38-L44); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @F.udf(returnType=ArrayType(StringType()))

import re


def extract_tags(tags_str):
    if tags_str is None:
        return []
    # Remove < dan > lalu split
    tags = re.findall(r'<([^>]+)>', tags_str)
    return tags


import json


def case_entry(tags_str):
    return json.dumps(extract_tags(tags_str))
