# codon-fuzz: entry=get_h3_index args=VARSIZED,VARSIZED,INT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["52.520008", "13.404954", 10], ["40.748817", "-73.985428", 9], ["37.775938728915946", "-122.41795063018799", 9], ["-33.8688", "151.2093", 7], ["48.8584", "2.2945", 12], ["35.6762", "139.6503", 5]]
# real-udf: origin=github-spark id=kuwala_get_h3_index requires=h3 wrapped=0 ported=0
# Note: needs the h3 3.x API (geo_to_h3).
# Source: https://github.com/kuwala-io/kuwala/blob/44d73c2cd0be03acc5984b79597f40695fdeff4c/kuwala/common/python_utils/src/spark_udfs.py#L21-L27
# Repository: kuwala-io/kuwala @ commit 44d73c2cd0be03acc5984b79597f40695fdeff4c
# License: Apache-2.0
# Copied verbatim from kuwala/common/python_utils/src/spark_udfs.py (L21-L27); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

import h3


def get_h3_index(lat: str, lng: str, resolution):
    try:
        # noinspection PyUnresolvedReferences
        return h3.geo_to_h3(float(lat), float(lng), resolution)
    except TypeError:
        return None
