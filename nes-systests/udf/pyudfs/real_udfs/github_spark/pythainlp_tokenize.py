# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["ผมรักภาษาไทย"], ["วันนี้อากาศดีมาก"], ["กรุงเทพมหานครเป็นเมืองหลวงของประเทศไทย"], ["ฉันกินข้าวแล้ว"], ["Spark กับ PyThaiNLP"]]
# real-udf: origin=github-spark id=pythainlp_tokenize requires=pythainlp wrapped=1 ported=0
# Wrapper: the UDF returns an array of tokens; case_entry returns it as JSON text.
# Source: https://github.com/PyThaiNLP/pythainlp/blob/3d232cc7063b60199f75fac3dfe24b8fbc278063/examples/distributed_pyspark.py#L88-L101
# Repository: PyThaiNLP/pythainlp @ commit 3d232cc7063b60199f75fac3dfe24b8fbc278063
# License: Apache-2.0
# Copied verbatim from examples/distributed_pyspark.py (L88-L101); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=ArrayType(StringType()))

def tokenize_udf(text):
    """
    UDF for tokenizing Thai text.

    IMPORTANT: Set environment variable and import inside the UDF.
    """
    import os

    os.environ["PYTHAINLP_DATA"] = "./pythainlp-data"

    from pythainlp.tokenize import word_tokenize

    return word_tokenize(text)


import json


def case_entry(text):
    return json.dumps(tokenize_udf(text), ensure_ascii=False)
