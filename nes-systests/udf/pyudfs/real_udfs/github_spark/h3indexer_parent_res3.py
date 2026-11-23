# codon-fuzz: entry=get_parent_h3_res3 args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["8928308280fffff"], ["8828308281fffff"], ["872830828ffffff"], ["85283473fffffff"], ["8a2a1072b59ffff"], ["891fb46622fffff"]]
# real-udf: origin=github-spark id=h3indexer_parent_res3 requires=h3 wrapped=0 ported=0
# Note: needs the h3 3.x API (h3_to_parent).
# Source: https://github.com/amazon-science/h3-indexer/blob/dc822ed87a79942e49031f3c727e9fa7c08257e9/src/h3_indexer/spark/spark_udfs.py#L12-L21
# Repository: amazon-science/h3-indexer @ commit dc822ed87a79942e49031f3c727e9fa7c08257e9
# License: Apache-2.0
# Copied verbatim from src/h3_indexer/spark/spark_udfs.py (L12-L21); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

import h3


def get_parent_h3_res3(h3_index: str) -> str:
    """Get the parent of the H3 index at resolution 3.

    :param h3_index: H3 index - any resolution.
    :return: parent H3 index at resolution 3
    """
    if h3_index is None:
        return None
    return h3.h3_to_parent(h3_index, 3)
