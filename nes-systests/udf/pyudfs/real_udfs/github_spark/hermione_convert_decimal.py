# codon-fuzz: entry=convert_decimal_udf args=VARSIZED returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1.234,56"], ["0,75"], ["12.345.678,9"], ["100"], ["3,14159"], ["-2.500,00"], ["7.5"]]
# real-udf: origin=github-spark id=hermione_convert_decimal requires= wrapped=0 ported=0
# Source: https://github.com/A3Data/hermione/blob/c913f9235579a165505562d04afd828395081ec9/hermione/spark/spark_utils.py#L26-L33
# Repository: A3Data/hermione @ commit c913f9235579a165505562d04afd828395081ec9
# License: Apache-2.0
# Copied verbatim from hermione/spark/spark_utils.py (L26-L33); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @f.udf(returnType=t.DoubleType())

def convert_decimal_udf(string):
    """Spark UDF to convert comma-separated decimal numbers to point-separated decimal numbers"""
    if string is None:
        return None
    else:
        string = string.replace(",", ".")
        return float(string.replace(".", "", string.count(".") - 1))
