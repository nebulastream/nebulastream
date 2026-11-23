# codon-fuzz: entry=unidecode_udf args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["São Paulo"], ["Ribeirão Preto"], ["Avaliação técnica"], ["Größe"], ["Ação"], ["Zürich – Genève"]]
# real-udf: origin=github-spark id=hermione_unidecode requires=unidecode wrapped=0 ported=0
# Source: https://github.com/A3Data/hermione/blob/c913f9235579a165505562d04afd828395081ec9/hermione/spark/spark_utils.py#L17-L23
# Repository: A3Data/hermione @ commit c913f9235579a165505562d04afd828395081ec9
# License: Apache-2.0
# Copied verbatim from hermione/spark/spark_utils.py (L17-L23); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @f.udf(returnType=t.StringType())

from unidecode import unidecode


def unidecode_udf(string):
    """Spark UDF to remove special characters from string columns"""
    if not string:
        return None
    else:
        return unidecode(string)
