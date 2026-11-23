# codon-fuzz: entry=case_entry args=VARSIZED,INT64 returns=INT64
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["2016-01-15T08:07:30+00:00", 15], ["2016-01-15T08:15:00+00:00", 15], ["2016-02-29T23:59:59.250000+00:00", 15], ["2016-03-01T10:31:00+00:00", 30], ["2016-03-01T10:01:45+00:00", 60], ["2016-06-30T06:44:59.999999+00:00", 5], ["2016-12-31T23:50:00+00:00", 30]]
# real-udf: origin=github-spark id=azure_taxi_rounded_unix_ts requires= wrapped=1 ported=0
# Wrapper: Spark passes a timestamp; case_entry parses the ISO 8601 text (UTC offset included, so
# Wrapper: timestamp() does not depend on the local time zone) into a datetime.
# Source: https://github.com/Azure-Samples/azure-databricks-mlops-mlflow/blob/eeacd5223d21973628bda972dbaf476ddad14a28/ml_source/src/taxi_fares/utils/pyspark_utils.py#L33-L39
# Repository: Azure-Samples/azure-databricks-mlops-mlflow @ commit eeacd5223d21973628bda972dbaf476ddad14a28
# License: MIT
# Copied verbatim from ml_source/src/taxi_fares/utils/pyspark_utils.py (L33-L39); only the module's own import lines that the code uses are prepended.

import math
from datetime import datetime, timedelta


def rounded_unix_timestamp(dt, num_minutes=15):
    """
    Ceilings datetime dt to interval num_minutes, then returns the unix timestamp.
    """
    nsecs = dt.minute * 60 + dt.second + dt.microsecond * 1e-6
    delta = math.ceil(nsecs / (60 * num_minutes)) * (60 * num_minutes) - nsecs
    return int((dt + timedelta(seconds=delta)).timestamp())


def case_entry(pickup_datetime, num_minutes):
    return rounded_unix_timestamp(datetime.fromisoformat(pickup_datetime), num_minutes)
