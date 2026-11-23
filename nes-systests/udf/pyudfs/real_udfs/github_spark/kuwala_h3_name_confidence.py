# codon-fuzz: entry=get_confidence_based_h3_and_name_distance args=INT64,INT64,INT64 returns=FLOAT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[10, 85, 500], [0, 90, 500], [100, 70, 500], [600, 50, 500], [250, 100, 500], [26, 0, 300], [499, 33, 500]]
# real-udf: origin=github-spark id=kuwala_h3_name_confidence requires= wrapped=0 ported=0
# Source: https://github.com/kuwala-io/kuwala/blob/44d73c2cd0be03acc5984b79597f40695fdeff4c/kuwala/common/python_utils/src/spark_udfs.py#L110-L130
# Repository: kuwala-io/kuwala @ commit 44d73c2cd0be03acc5984b79597f40695fdeff4c
# License: Apache-2.0
# Copied verbatim from kuwala/common/python_utils/src/spark_udfs.py (L110-L130); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=DoubleType())

def get_confidence_based_h3_and_name_distance(
    h3_distance: int, name_distance: int, max_h3_distance: int
):
    def get_h3_confidence(d):
        if d <= 25:
            return 1

        return 1 - d / max_h3_distance if d < max_h3_distance else 0

    def get_name_confidence(d):
        return d / 100

    h3_confidence = get_h3_confidence(h3_distance) if h3_distance else None
    name_confidence = get_name_confidence(name_distance)

    return (
        (h3_confidence * (2 / 3) + name_confidence * (1 / 3))
        if h3_confidence
        else name_confidence
    )
