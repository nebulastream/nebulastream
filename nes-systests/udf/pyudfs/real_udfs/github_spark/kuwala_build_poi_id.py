# codon-fuzz: entry=build_poi_id_based_on_confidence args=FLOAT64,VARSIZED,VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[0.95, "8a1f05a4c4a7fff", "8a1f05a4c4affff", 2345678901], [0.5, "8a1f05a4c4a7fff", "8a1f05a4c4affff", 2345678901], [0.9, "8a2a1072b59ffff", "8a2a1072b597fff", 123456], [0.0, "8a2a1072b59ffff", "8a2a1072b597fff", 987654321], [0.8999, "8928308280fffff", "8928308280bffff", 42]]
# real-udf: origin=github-spark id=kuwala_build_poi_id requires= wrapped=0 ported=0
# Source: https://github.com/kuwala-io/kuwala/blob/44d73c2cd0be03acc5984b79597f40695fdeff4c/kuwala/common/python_utils/src/spark_udfs.py#L134-L139
# Repository: kuwala-io/kuwala @ commit 44d73c2cd0be03acc5984b79597f40695fdeff4c
# License: Apache-2.0
# Copied verbatim from kuwala/common/python_utils/src/spark_udfs.py (L134-L139); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

def build_poi_id_based_on_confidence(confidence, h3_index_google, h3_index_osm, osm_id):
    if confidence and confidence >= 0.9:
        return f"{h3_index_google}_{osm_id}"

    return f"{h3_index_osm}_{osm_id}"
