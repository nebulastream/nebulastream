# codon-fuzz: entry=get_bucket args=FLOAT64 returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [[4500.0], [3999.99], [4000.0], [0.0], [12873.5], [250.0]]
# real-udf: origin=databases id=bigquery_docs_get_bucket requires= wrapped=0 ported=0
# Source: https://docs.cloud.google.com/bigquery/docs/user-defined-functions-python
# Commit: n/a (documentation page, fetched 2026-11-23)
# License: Apache-2.0
# Google Cloud documentation; per the Google Developers Site Policy, code samples are licensed under Apache-2.0.

def get_bucket(num: float) -> str:
    if not num:
        return "NA"
    boundary = 4000
    return "at_or_above_4000" if num >= boundary else "below_4000"
