# codon-fuzz: entry=case_entry args=VARSIZED returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2016-01-01T14:20:00+00:00"], ["2016-01-02T03:15:00+00:00"], ["2016-01-02T18:40:00+00:00"], ["2016-01-03T23:59:00+00:00"], ["2016-01-04T04:30:00+00:00"], ["2016-06-11T12:00:00+00:00"], ["2016-02-29T09:05:13+00:00"]]
# real-udf: origin=github-spark id=azure_taxi_is_weekend requires=pytz wrapped=1 ported=0
# Wrapper: Spark passes a timestamp; case_entry parses the ISO 8601 text (UTC offset included) into a datetime.
# Source: https://github.com/Azure-Samples/azure-databricks-mlops-mlflow/blob/eeacd5223d21973628bda972dbaf476ddad14a28/ml_source/src/taxi_fares/utils/pyspark_utils.py#L11-L14
# Repository: Azure-Samples/azure-databricks-mlops-mlflow @ commit eeacd5223d21973628bda972dbaf476ddad14a28
# License: MIT
# Copied verbatim from ml_source/src/taxi_fares/utils/pyspark_utils.py (L11-L14); only the module's own import lines that the code uses are prepended.
# Spark decorator(s) removed so the file is plain Python: @udf(returnType=IntegerType())

from pytz import timezone


def is_weekend(dt: Column) -> Column:
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday


from datetime import datetime as _datetime


def case_entry(pickup_datetime):
    return is_weekend(_datetime.fromisoformat(pickup_datetime))
