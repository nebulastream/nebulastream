# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2016-01-15 08:07:30"], ["2016-02-29 23:59:59"], ["2015-12-31 00:00:00"], ["2016-10-03 17:45:12"], ["2009-07-04 12:00:00"], ["2016-06-30 06:30:00"]]
# real-udf: origin=github-spark id=azure_taxi_partition_id requires= wrapped=1 ported=0
# Wrapper: Spark passes a timestamp; case_entry parses the ISO 8601 text into a datetime.
# Source: https://github.com/Azure-Samples/azure-databricks-mlops-mlflow/blob/eeacd5223d21973628bda972dbaf476ddad14a28/ml_source/src/taxi_fares/utils/pyspark_utils.py#L17-L20
# Repository: Azure-Samples/azure-databricks-mlops-mlflow @ commit eeacd5223d21973628bda972dbaf476ddad14a28
# License: MIT
# Copied verbatim from ml_source/src/taxi_fares/utils/pyspark_utils.py (L17-L20); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=StringType())

def partition_id(dt: Column) -> Column:
    # datetime -> "YYYY-MM"
    return f"{dt.year:04d}-{dt.month:02d}"


from datetime import datetime as _datetime


def case_entry(pickup_datetime):
    return partition_id(_datetime.fromisoformat(pickup_datetime))
